import os

##################
# Container Configurations (must be present)
CONTAINER_ID = os.getenv("CONTAINER_ID")
assert CONTAINER_ID is not None, "Environment variable CONTAINER_ID must be set"
print(f"Configured environment variable CONTAINER_ID as '{CONTAINER_ID}'")

CONTAINER_PORT = int(os.getenv("CONTAINER_PORT"))
assert CONTAINER_PORT is not None, "Environment variable PORT must be set"
print(f"Configured environment variable PORT as {CONTAINER_PORT}")

##################
# Pipeline Configurations
MAX_FAILS = int(os.getenv("MAX_FAILS", 1))
print(f"Configured environment variable MAX_FAILS as {MAX_FAILS}")

SQL_BATCH_SIZE = int(os.getenv("SQL_BATCH_SIZE", 200))
print(f"Configured environment variable SQL_BATCH_SIZE as {SQL_BATCH_SIZE}")
