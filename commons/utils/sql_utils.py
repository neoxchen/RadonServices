import traceback
from contextlib import contextmanager
from typing import ContextManager

import numpy as np
from psycopg2 import extensions, DatabaseError
from psycopg2.extensions import register_adapter, AsIs
from psycopg2.pool import ThreadedConnectionPool

# Register numpy -> postgres value adapter upon import
register_adapter(np.float64, lambda float64: AsIs(float64))
register_adapter(np.int64, lambda int64: AsIs(int64))


class PostgresClient:
    def __init__(self, host: str, user: str, password: str, port: str, database: str, min_connections: int = 1, max_connections: int = 10):
        self.host: str = host
        self.user: str = user
        self.password: str = password
        self.port: str = port
        self.database: str = database
        self.min_connections: int = min_connections
        self.max_connections: int = max_connections

        # Initialize connection pool
        self.connection_pool: ThreadedConnectionPool = ThreadedConnectionPool(
            minconn=self.min_connections,
            maxconn=self.max_connections,
            host=self.host,
            user=self.user,
            password=self.password,
            port=self.port,
            database=self.database
        )

    @contextmanager
    def cursor(self) -> ContextManager[extensions.cursor]:
        postgres_connection: extensions.connection = self.connection_pool.getconn()
        try:
            postgres_cursor: extensions.cursor = postgres_connection.cursor()
            try:
                yield postgres_cursor
            finally:
                postgres_cursor.close()
            postgres_connection.commit()
        except DatabaseError as error:
            print("Rolling back all transactions because of database error")
            print(f"Error: {error}")
            print(f"Traceback: {traceback.format_exc()}")
            postgres_connection.rollback()
        finally:
            self.connection_pool.putconn(postgres_connection)


class AbstractPostgresClientFactory:
    def create(self) -> PostgresClient:
        raise NotImplementedError


class ClothoDockerPostgresClientFactory(AbstractPostgresClientFactory):
    def create(self) -> PostgresClient:
        return PostgresClient(
            host="postgres",
            port="5432",
            user="radon",
            password="radon2023",
            database="radon_sql_v4"
        )


class LocalPostgresClientFactory(AbstractPostgresClientFactory):
    def create(self) -> PostgresClient:
        return PostgresClient(
            host="localhost",
            port="5432",
            user="radon",
            password="radon2023",
            database="radon_sql_v4"
        )


if __name__ == "__main__":
    from time import sleep

    start_container = False
    if start_container:
        import docker
        from docker.types import Mount

        print("Creating a Docker client...")
        docker_client: docker.client.DockerClient = docker.from_env()

        print("Pulling the latest postgres image...")
        docker_client.images.pull("postgres", tag="latest")

        host_port = 5432
        print(f"Starting a postgres container at port {host_port}...")

        mount = Mount(target="/var/lib/postgresql/data", source="C:/One/UCI/Alberto/data/postgres", type="bind")
        new_container_object = docker_client.containers.run(
            name="radon-postgres",
            image="postgres:latest",
            ports={f"5432/tcp": host_port},
            environment={
                "POSTGRES_USER": "radon",
                "POSTGRES_PASSWORD": "radon2023",
                "POSTGRES_DB": "radon_sql_v4"
            },
            mounts=[mount],
            detach=True
        )

        print("Waiting 30 seconds for the container to start...")
        sleep(30)
    else:
        print("Skipping container start sequence")

    print("Establishing a connection to the database...")
    postgres_client_factory = LocalPostgresClientFactory()
    postgres_client = postgres_client_factory.create()

    print("Updating the status of all galaxies to 'Pending'...")
    with postgres_client.cursor() as cursor:
        cursor.execute("""
            UPDATE galaxies
            SET status = 'Pending'
        """)
