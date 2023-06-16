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
    host="postgres",  # TODO: change to Docker container name
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
