import traceback
from contextlib import contextmanager
from typing import Generator

import numpy as np
from psycopg2 import DatabaseError
from psycopg2.extensions import cursor, register_adapter, AsIs
from psycopg2.pool import ThreadedConnectionPool

# Register numpy -> postgres value adapter
register_adapter(np.float64, lambda float64: AsIs(float64))
register_adapter(np.int64, lambda int64: AsIs(int64))

# Create connection pool
pool = ThreadedConnectionPool(
    minconn=1,
    maxconn=10,
    host="postgres",  # Docker container name
    user="radon",
    password="radon2023",
    port="5432",
    database="radon_sql"
)


@contextmanager
def postgres() -> Generator[cursor, None, None]:
    connection = pool.getconn()
    try:
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()
        connection.commit()
    except DatabaseError as error:
        print("Rolling back all transactions because of database error")
        print(f"Error: {error}")
        print(f"Traceback: {traceback.format_exc()}")
        connection.rollback()
    finally:
        pool.putconn(connection)


if __name__ == "__main__":
    # Creates the two data tables as an example of how to use this class

    # noinspection SqlNoDataSourceInspection
    create_galaxies = """
        CREATE TABLE galaxies (
            id              SERIAL PRIMARY KEY NOT NULL,
            source_id       VARCHAR            NOT NULL UNIQUE,
            ra              DECIMAL(25, 20)    NOT NULL,
            dec             DECIMAL(25, 20)    NOT NULL,
            gal_prob        DECIMAL(25, 20)    NOT NULL,
            bin             SMALLINT           NOT NULL,
            status          VARCHAR            NOT NULL DEFAULT 'Pending',
            failed_attempts SMALLINT           NOT NULL DEFAULT 0
        );"""

    # noinspection SqlNoDataSourceInspection
    create_fits = """
        CREATE TABLE fits_data (
            source_id      VARCHAR PRIMARY KEY REFERENCES galaxies (source_id),
            has_data_g     BOOLEAN NOT NULL,
            has_data_r     BOOLEAN NOT NULL,
            has_data_i     BOOLEAN NOT NULL,
            has_data_z     BOOLEAN NOT NULL,
            fits_file_path VARCHAR NOT NULL
        );"""

    with postgres() as cursor:
        cursor.execute(create_galaxies)
        cursor.execute(create_fits)

    print("Successfully created galaxies and fits_data tables!")
