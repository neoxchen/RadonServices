import os
from time import sleep
from typing import List, Tuple

import numpy as np
from astropy.io import fits
from psycopg2 import extensions
from tqdm import tqdm

from augmenter import random_augment
from parallel_util import run_in_parallel
from radon import estimate_rotation
from running_error import RunningErrorCalculator
from sql_util import postgres

# Fetch environment variables or use defaults
CONTAINER_ID = os.getenv("CONTAINER_ID")
print(f"Configured environment variable CONTAINER_ID as '{CONTAINER_ID}'")

DATA_PATH = os.getenv("DATA_PATH", "/fits-data")
print(f"Configured environment variable DATA_PATH as '{DATA_PATH}'")

AUGMENTATION_COUNT = int(os.getenv("AUGMENTATION_COUNT", 50))
print(f"Configured environment variable AUGMENTATION_COUNT as {AUGMENTATION_COUNT}")

THREAD_COUNT = int(os.getenv("THREAD_COUNT", 32))
print(f"Configured environment variable THREAD_COUNT as {THREAD_COUNT}")

MAX_RUNNING_ERROR_COUNT = int(os.getenv("MAX_RUNNING_ERROR_COUNT", 50))
assert MAX_RUNNING_ERROR_COUNT > 0 and MAX_RUNNING_ERROR_COUNT % AUGMENTATION_COUNT == 0, \
    f"MAX_RUNNING_ERROR_COUNT {MAX_RUNNING_ERROR_COUNT} must be a multiple of AUGMENTATION_COUNT {AUGMENTATION_COUNT}"
print(f"Configured environment variable MAX_RUNNING_ERROR_COUNT as {MAX_RUNNING_ERROR_COUNT}")

# Dynamic flag to stop the script
stop_script = False


def set_stop_script():
    global stop_script
    stop_script = True


def load_fits(source_id: str, bin_id: str, band: str) -> np.ndarray:
    fits_path = f"{DATA_PATH}/b{bin_id}/{source_id}.fits"
    with fits.open(fits_path) as hdu_list:
        fits_data_list = hdu_list[0].data
        hdu_list.close()

    # Returns only one band's FITS data
    band_index = "griz".index(band)
    return fits_data_list[band_index]


def generate_augmentations(fits_data: np.ndarray) -> List[Tuple[np.ndarray, int]]:
    """ Generates AUGMENTATION_COUNT number of augmented images, takes in 1 band data """
    return [random_augment(fits_data) for _ in range(AUGMENTATION_COUNT)]


def parallel_process(source_id: str, bin_id: str, band: str) -> RunningErrorCalculator:
    """ Processes a single galaxy band, can be run in parallel """
    # Load FITS data
    band_fits_data = load_fits(source_id, bin_id, band)

    # Calculate "oracle" rotation
    ideal_rotation = estimate_rotation(band_fits_data)

    # Generate augmentations & estimate error
    augmentations = generate_augmentations(band_fits_data)
    running_error_calculator = RunningErrorCalculator()

    for augmented_fits, delta_rotation in augmentations:
        actual_rotation = estimate_rotation(augmented_fits)
        running_error_calculator.update(ideal_rotation + delta_rotation, actual_rotation)

    return running_error_calculator


def insert_result(cursor: extensions.cursor, source_id: str, band: str, error: RunningErrorCalculator) -> None:
    # Assuming cursor is managed with a 'with' statement
    # cursor.execute(
    #     """
    #         UPDATE bands
    #         SET total_error = total_error + %s,
    #             running_count = running_count + %s
    #         WHERE source_id =%s AND band=%s
    #     """,
    #     (error.total_error, error.running_count, source_id, band, error.get_average())
    # )
    print(f"Pseudo-insert: galaxy {source_id} band {band} error: {error}")


def run_script():
    print("Starting angle-error estimation pipeline script...")
    iteration = 1
    while not stop_script:
        # Minor sleep
        sleep(2)

        print(f"Processing batch #{iteration}...")
        with postgres() as cursor:
            cursor: extensions.cursor

            # Fetch batch from 'galaxies' table
            cursor.execute(f"""
                SELECT g.source_id, g.bin, b.band
                FROM galaxies g
                JOIN bands b ON g.source_id = b.source_id
                WHERE b.running_count < {MAX_RUNNING_ERROR_COUNT}
                ORDER BY g.id, b.id
                LIMIT {THREAD_COUNT}
                FOR UPDATE SKIP LOCKED
            """)
            metadata_list = cursor.fetchall()

            # If we've completed processing of all fetched galaxies, stop loop
            if not metadata_list:
                set_stop_script()
                break

            pbar = tqdm(metadata_list, desc=f"It #{iteration}", total=len(metadata_list))
            error_calculators = run_in_parallel(parallel_process, [[metadata] for metadata in metadata_list], THREAD_COUNT, lambda: pbar.update(1))

            for metadata, error_calculator in zip(metadata_list, error_calculators):
                source_id, bin_id, band = metadata
                insert_result(cursor, source_id, band, error_calculator)

            pbar.close()

        # Call API to update status
        # print(f"Updating pipeline status for iteration #{iteration}...")
        # requests.post(f"http://backend:5000/pipelines/status/{CONTAINER_ID}", json={
        #     "iteration": iteration,
        #     "galaxies": [result_entry.source_id for result_entry in result_rows if not result_entry.is_error],
        #     "errors": [result_entry.source_id for result_entry in result_rows if result_entry.is_error]
        # })
        #
        # print(f"Iteration #{iteration} ended with {len(result_rows)} galaxies processed")
        iteration += 1

    # Signal pipeline shutdown
    # requests.delete(f"http://backend:5000/pipelines/status/{CONTAINER_ID}")
    print("Angle-error estimation pipeline script execution complete!")


if __name__ == "__main__":
    run_script()
