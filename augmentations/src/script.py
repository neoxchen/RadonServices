import os
from time import sleep
from typing import List, Tuple, Optional

import numpy as np
import requests
from astropy.io import fits
from psycopg2 import extensions
from tqdm import tqdm

from running_error import RunningErrorCalculator
from src.augmenter import random_augment
from src.parallel_util import run_in_parallel
from src.radon import estimate_rotation

# Fetch environment variables or use defaults
CONTAINER_ID = os.getenv("CONTAINER_ID")
print(f"Configured environment variable CONTAINER_ID as '{CONTAINER_ID}'")

DATA_PATH = os.getenv("DATA_PATH", "/fits-data")
print(f"Configured environment variable DATA_PATH as '{DATA_PATH}'")

AUGMENTATION_COUNT = int(os.getenv("AUGMENTATION_COUNT", 100))
print(f"Configured environment variable AUGMENTATION_COUNT as {AUGMENTATION_COUNT}")

THREAD_COUNT = int(os.getenv("THREAD_COUNT", 64))
print(f"Configured environment variable THREAD_COUNT as {THREAD_COUNT}")

MAX_RUNNING_ERROR_COUNT = int(os.getenv("MAX_RUNNING_ERROR_COUNT", 100))
assert MAX_RUNNING_ERROR_COUNT > 0 and MAX_RUNNING_ERROR_COUNT % AUGMENTATION_COUNT == 0, \
    f"MAX_RUNNING_ERROR_COUNT {MAX_RUNNING_ERROR_COUNT} must be a multiple of AUGMENTATION_COUNT {AUGMENTATION_COUNT}"
print(f"Configured environment variable MAX_RUNNING_ERROR_COUNT as {MAX_RUNNING_ERROR_COUNT}")

# Dynamic flag to stop the script
stop_script = False


def set_stop_script():
    global stop_script
    stop_script = True


def load_fits(source_id: str, bin_id: str, band: str) -> Optional[np.ndarray]:
    fits_path = f"{DATA_PATH}/b{bin_id}/{source_id}.fits"
    with fits.open(fits_path) as hdu_list:
        fits_data_list = hdu_list[0].data
        hdu_list.close()

    # Returns only one band's FITS data
    band_index = "griz".index(band)
    band_fits = fits_data_list[band_index]

    # Check validity of FITS data
    if not band_fits.any() or np.any(np.isnan(band_fits)):
        return None

    # Shift FITS data to be non-negative
    shift = np.min(band_fits)
    if shift < 0:
        band_fits += abs(shift)

    return band_fits


def generate_augmentations(fits_data: np.ndarray) -> List[Tuple[np.ndarray, int]]:
    """ Generates AUGMENTATION_COUNT number of augmented images, takes in 1 band data """
    return [random_augment(fits_data) for _ in range(AUGMENTATION_COUNT)]


def parallel_process(source_id: str, bin_id: str, band: str) -> Tuple[Optional[RunningErrorCalculator], bool]:
    """ Processes a single galaxy band, can be run in parallel """
    try:
        # Load FITS data
        band_fits_data = load_fits(source_id, bin_id, band)
        if band_fits_data is None:
            raise Exception("FITS data is invalid, unable to load & preprocess")

        # Calculate "oracle" rotation
        ideal_rotation = estimate_rotation(band_fits_data)

        # Generate augmentations & estimate error
        augmentations = generate_augmentations(band_fits_data)
        running_error_calculator = RunningErrorCalculator()

        for augmented_fits, delta_rotation in augmentations:
            actual_rotation = estimate_rotation(augmented_fits)
            running_error_calculator.update(ideal_rotation + delta_rotation, actual_rotation)

        return running_error_calculator, False
    except Exception as e:
        print(f"Error processing source_id={source_id}, bin_id={bin_id}, band={band}")
        print(e, flush=True)

        # Set error flag
        return None, True


def insert_result(cursor: extensions.cursor, rotation_id: int, error: RunningErrorCalculator) -> None:
    # Assuming cursor is managed with a 'with' statement
    cursor.execute("""
        UPDATE rotations
        SET total_error = total_error + %s,
            running_count = running_count + %s
        WHERE id = %s
    """, (error.total_error, error.running_count, rotation_id))


def insert_error(cursor: extensions.cursor, band_id: int) -> None:
    # Assuming cursor is managed with a 'with' statement
    cursor.execute("""
        UPDATE bands
        SET error_count = error_count + 1
        WHERE id = %s
    """, (band_id,))


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
                SELECT b.id, r.id, g.source_id, g.bin, b.band
                FROM galaxies g
                    INNER JOIN bands b ON g.source_id = b.source_id
                    INNER JOIN rotations r ON b.id = r.band_id
                WHERE r.running_count < {MAX_RUNNING_ERROR_COUNT}
                    AND b.error_count = 0
                ORDER BY b.id
                LIMIT {THREAD_COUNT}
                FOR UPDATE SKIP LOCKED
            """)
            # List of [band_id, rotation_id, source_id, bin_id, band]
            metadata_list = cursor.fetchall()

            # If we've completed processing of all fetched galaxies, stop loop
            if not metadata_list:
                set_stop_script()
                break

            pbar = tqdm(metadata_list, desc=f"It #{iteration}", total=len(metadata_list))
            process_results = run_in_parallel(parallel_process, [[*metadata[2:]] for metadata in metadata_list],
                                              THREAD_COUNT, lambda: pbar.update(1))

            successful, failed = 0, 0
            for metadata, process_result in zip(metadata_list, process_results):
                error_calculator, is_error = process_result
                if not is_error:
                    insert_result(cursor, metadata[0], error_calculator)
                    successful += 1
                else:
                    insert_error(cursor, metadata[1])
                    failed += 1

            pbar.close()

        print(f"Processed {successful} galaxies successfully, {failed} failed, total {len(metadata_list)} galaxies")

        # Call API to update status
        print(f"Updating pipeline status for iteration #{iteration}...")
        requests.post(f"http://backend:5000/pipelines/status/{CONTAINER_ID}", json={
            "iteration": iteration,
            "successful": successful,
            "failed": failed
        })
        iteration += 1

    # Signal pipeline shutdown
    # requests.delete(f"http://backend:5000/pipelines/status/{CONTAINER_ID}")
    print("Angle-error estimation pipeline script execution complete!")


if __name__ == "__main__":
    run_script()
