from commons.models.fits_interfaces import LinuxFitsInterface
from commons.orchestration.pipeline import Pipeline
from commons.utils.sql_utils import ClothoDockerPostgresClientFactory
from constants import CONTAINER_ID, CONTAINER_PORT
from script import FetchScript

if __name__ == "__main__":
    # This file is the Docker container's entrypoint
    # postgres_factory = LocalPostgresClientFactory()
    # fits_interface = LocalTestingFitsInterface()

    postgres_factory = ClothoDockerPostgresClientFactory()
    fits_interface = LinuxFitsInterface()

    script = FetchScript(postgres_factory, fits_interface)
    pipeline = Pipeline(CONTAINER_ID, CONTAINER_PORT, script)
    pipeline.start()
