import csv
import glob
import os.path

import psycopg2
from psycopg2.extras import DictCursor, execute_values

from listenbrainz import config


def import_recording_similarity():
    with psycopg2.connect(config.MBID_MAPPING_DATABASE_URI) as mb_conn:
        for size in [1, 5, 10]:
            with mb_conn.cursor(cursor_factory=DictCursor) as mb_curs:
                table = f"mapping.recording_similarity_index_{size}"
                mb_curs.execute(f"""
                    CREATE TABLE mapping.recording_similarity_index_{size} (
                        mbid0           UUID NOT NULL,
                        mbid1           UUID NOT NULL,
                        similarity      DOUBLE PRECISION NOT NULL
                    )
                """)

                directory = f"~/recording_similarity_index/{size}"
                for path in glob.glob(os.path.join(directory, "*.csv")):
                    with open(path, newline='') as csvfile:
                        index = csv.reader(csvfile)
                        values = (tuple(row) for row in index)

                        query = f"INSERT INTO {table} VALUES %s"
                        template = "(%s::UUID, %s::UUID, %s)"

                        execute_values(mb_curs, query, values, template)

                mb_conn.commit()
