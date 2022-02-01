import logging
from datetime import datetime

import pyspark.sql

import listenbrainz_spark
from listenbrainz_spark.constants import LAST_FM_FOUNDING_YEAR
from listenbrainz_spark.schema import recording_similarity_index_schema
from listenbrainz_spark.stats import run_query
from listenbrainz_spark.utils import get_listens_from_new_dump

LOOKAHEAD_STEPS = 5
DECREMENT = 1.0 / LOOKAHEAD_STEPS
SIMILARITY_THRESHOLD = 10.0

logger = logging.getLogger(__name__)


def calculate():
    from_date, to_date = datetime(LAST_FM_FOUNDING_YEAR, 1, 1), datetime.now()
    listens_df = get_listens_from_new_dump(from_date, to_date)
    table = "rec_sim_listens"
    listens_df.createOrReplaceTempView(table)

    scattered_df: pyspark.sql.DataFrame = listenbrainz_spark.session.createDataFrame([], recording_similarity_index_schema)

    weight = 1.0
    for idx in range(LOOKAHEAD_STEPS):
        query = f"""
            WITH mbid_similarity AS (
                SELECT recording_mbid AS mbid0
                     , LEAD(recording_mbid, {idx}) OVER row_next AS mbid1
                     , (artist_credit_id != LEAD(artist_credit_id, {idx}) OVER row_next AND recording_mbid != LEAD(recording_mbid, {idx}) OVER row_next) AS similar
                  FROM {table}
                 WHERE recording_mbid IS NOT NULL 
                WINDOW row_next AS (PARTITION BY user_name ORDER BY listened_at)
            )
            SELECT mbid0
                 , mbid1
                 , SUM(CASE WHEN similar THEN 1 ELSE 0 END) * {weight} AS similarity
              FROM mbid_similarity
             WHERE mbid0 IS NOT NULL
               AND mbid1 IS NOT NULL
          GROUP BY mbid0, mbid1
        """
        scattered_df.unionAll(run_query(query))
        weight -= DECREMENT
        logger.info("Scattered Count: %d", scattered_df.count())
        logger.info("Scattered Sample: %d", scattered_df.take(5))

    rec_sim_table = "recording_similarity_index_scattered"
    scattered_df.createOrReplaceTempView(rec_sim_table)
    rec_sim_query = f"""
        WITH symmetric_index AS (
            SELECT CASE WHEN mbid0 < mbid1 THEN mbid0 ELSE mbid1 END AS lexical_mbid0
                 , CASE WHEN mbid0 > mbid1 THEN mbid0 ELSE mbid1 END AS lexical_mbid1
                 , similarity
              FROM {rec_sim_table}   
        )
            SELECT lexical_mbid0 AS mbid0
                 , lexical_mbid1 AS mbid1
                 , SUM(similarity) AS total_similarity
              FROM symmetric_index
          GROUP BY lexical_mbid0, lexical_mbid1
            HAVING SUM(similarity) > {SIMILARITY_THRESHOLD}       
    """
    rec_sim_index_df = run_query(rec_sim_query)
    logger.info("Index Count: %d", rec_sim_index_df.count())
    logger.info("Index Sample: %d", rec_sim_index_df.take(5))
    rec_sim_index_df.write.csv("/recording_similarity_index")
