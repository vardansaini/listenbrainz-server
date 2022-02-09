#!/usr/bin/python3

import csv
import glob
import sys

import psycopg2
from psycopg2.extras import DictCursor, execute_values

from listenbrainz import config


def import_index(entity: str, size: int):
    with psycopg2.connect(config.MBID_MAPPING_DATABASE_URI) as mb_conn:
        with mb_conn.cursor(cursor_factory=DictCursor) as mb_curs:
            table = f"mapping.{entity}_similarity_index_{size}"
            mb_curs.execute(f"""
                CREATE TABLE IF NOT EXISTS mapping.{entity}_similarity_index_{size} (
                    mbid0           UUID NOT NULL,
                    mbid1           UUID NOT NULL,
                    similarity      DOUBLE PRECISION NOT NULL
                )
            """)

            for path in glob.glob(f"/home/lucifer/{entity}_similarity_index_{size}/*.csv"):
                with open(path, newline='') as csvfile:
                    index = csv.reader(csvfile)
                    values = (tuple(row) for row in index)

                    query = f"INSERT INTO {table} VALUES %s"
                    template = "(%s::UUID, %s::UUID, %s)"

                    execute_values(mb_curs, query, values, template)

            mb_conn.commit()


if __name__ == '__main__':
    _entity, _size = sys.argv[1], int(sys.argv[2])
    print(f"Importing index for {_entity} of window size {_size}")
    import_index(_entity, _size)
    print("Import complete.")
