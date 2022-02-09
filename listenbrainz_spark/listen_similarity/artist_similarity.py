import logging
from datetime import datetime

import pyspark.sql

import listenbrainz_spark
from listenbrainz_spark.constants import LAST_FM_FOUNDING_YEAR
from listenbrainz_spark.schema import recording_similarity_index_schema
from listenbrainz_spark.stats import run_query
from listenbrainz_spark.utils import get_listens_from_new_dump

logger = logging.getLogger(__name__)


def calculate(window_size: int, similarity_threshold: float, time_threshold: int):
    decrement = 1.0 / window_size

    from_date, to_date = datetime(LAST_FM_FOUNDING_YEAR, 1, 1), datetime.now()
    listens_df = get_listens_from_new_dump(from_date, to_date)
    base_table = "artist_sim_listens_base"
    listens_df.createOrReplaceTempView(base_table)
    
    explode_artist_mbid_query = f"""
        SELECT user_id
             , listened_at
             , artist_credit_id
             , artist_credit_mbids
          FROM {base_table}
         WHERE artist_credit_id IS NOT NULL 
    """
    table = "artist_sim_listens"
    run_query(explode_artist_mbid_query).createOrReplaceTempView(table)

    scattered_df: pyspark.sql.DataFrame = listenbrainz_spark.session.createDataFrame([], recording_similarity_index_schema)

    weight = 1.0
    for idx in range(1, window_size + 1):
        query = f"""
            WITH artist_credit_similarity AS (
                SELECT artist_credit_mbids AS mbids_0
                     , LEAD(artist_credit_mbids, {idx}) OVER row_next AS mbids_1
                     ,    ( artist_credit_id != LEAD(artist_credit_id, {idx}) OVER row_next
                        -- spark-sql supports interval types but pyspark doesn't so currently need to convert to bigints
                        AND BIGINT(LEAD(listened_at, {idx}) OVER row_next) - BIGINT(listened_at) <= {time_threshold}
                       ) AS similar
                  FROM {table}
                WINDOW row_next AS (PARTITION BY user_id ORDER BY listened_at)
            ), explode_1_mbid AS (  -- cannot explode two columns in 1 step so split into 2.
                SELECT explode(mbids_0) AS  mbid0
                     , mbids_1
                  FROM artist_credit_similarity
                 WHERE similar  -- trimming the list as soon as possible, no need to explode non-similar artist-mbids
            ), mbid_similarity AS (
                SELECT mbid0
                     , explode(mbids_1) AS mbid1
                  FROM explode_1_mbid
            ), symmetric_index AS (
                SELECT CASE WHEN mbid0 < mbid1 THEN mbid0 ELSE mbid1 END AS lexical_mbid0
                     , CASE WHEN mbid0 > mbid1 THEN mbid0 ELSE mbid1 END AS lexical_mbid1
                  FROM mbid_similarity
                 WHERE mbid0 IS NOT NULL
                   AND mbid1 IS NOT NULL
                   AND mbid0 != mbid1 -- due to partial artist credit match, the same mbid can occur in both columns for
                   -- a row. ignore these rows.
            )
            SELECT lexical_mbid0 AS mbid0
                 , lexical_mbid1 AS mbid1
                 , COUNT(*) * {weight} AS similarity
              FROM symmetric_index
          GROUP BY lexical_mbid0, lexical_mbid1
        """
        scattered_df = scattered_df.union(run_query(query))
        weight -= decrement
        logger.info("Count after iteration %d: %d", idx, scattered_df.count())

    artist_sim_table = "artist_similarity_index_scattered"
    scattered_df.createOrReplaceTempView(artist_sim_table)
    artist_sim_query = f"""
        SELECT mbid0
             , mbid1
             , SUM(similarity) AS total_similarity
          FROM {artist_sim_table}
      GROUP BY mbid0, mbid1
        HAVING total_similarity > {similarity_threshold}
    """
    artist_sim_index_df = run_query(artist_sim_query)
    logger.info("Index Count: %d", artist_sim_index_df.count())
    artist_sim_index_df.write.csv(f"/artist_similarity_index/{window_size}/", mode="overwrite")
