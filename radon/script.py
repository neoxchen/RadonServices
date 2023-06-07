import os
from time import sleep

import numpy as np
import requests
import scipy
from astropy.io import fits
from psycopg2 import extensions
from tqdm import tqdm

from sql_util import postgres

# Fetch environment variables or use defaults
DATA_PATH = os.getenv("DATA_PATH", "/fits-data")
print(f"Configured environment variable DATA_PATH as '{DATA_PATH}'")

SQL_BATCH_SIZE = int(os.getenv("SQL_BATCH_SIZE", 200))
print(f"Configured environment variable SQL_BATCH_SIZE as {SQL_BATCH_SIZE}")

# Radon variables
THETAS = np.linspace(0, np.pi, 181)
SIN_THETAS = np.sin(THETAS)
COS_THETAS = np.cos(THETAS)

# Define the circle mask
SHAPE = (40, 40)
CENTER = [dim // 2 for dim in SHAPE]
RADIUS = SHAPE[0] // 2
Y_COORDS, X_COORDS = np.ogrid[-CENTER[0]:SHAPE[0] - CENTER[0], -CENTER[1]:SHAPE[1] - CENTER[1]]
MASK = X_COORDS * X_COORDS + Y_COORDS * Y_COORDS <= RADIUS * RADIUS


def radon_transform(image: np.ndarray) -> np.ndarray:
    """ Assuming image is already circle-masked """
    sinogram = np.zeros((40, len(THETAS)))
    for i, (sin_theta, cos_theta) in enumerate(zip(SIN_THETAS, COS_THETAS)):
        matrix = np.array([
            [cos_theta, sin_theta, -20 * (cos_theta + sin_theta - 1)],
            [-sin_theta, cos_theta, -20 * (cos_theta - sin_theta - 1)],
            [0, 0, 1]
        ])
        y_coords, x_coords = np.indices((40, 40))
        homogeneous_coords = np.stack((x_coords, y_coords, np.ones_like(x_coords)), axis=-1)
        homogeneous_coords_2d = homogeneous_coords.reshape((-1, 3))
        transformed_coords = np.dot(homogeneous_coords_2d, matrix.T)
        transformed_coords_2d = transformed_coords[:, :2] / transformed_coords[:, 2, np.newaxis]
        transformed_coords_3d = transformed_coords_2d.reshape((40, 40, 2))
        interpolated_image = scipy.ndimage.map_coordinates(image, transformed_coords_3d.transpose((2, 0, 1)))
        sums = np.sum(interpolated_image, axis=0)
        sinogram[:, i] = sums
    return sinogram


def process(metadata_entry):
    source_id, bin_id = metadata_entry

    # Fetch FITS file from disk
    fits_path = f"{DATA_PATH}/b{bin_id}/{source_id}.fits"
    with fits.open(fits_path) as hdu_list:
        data = hdu_list[0].data
        hdu_list.close()

    # Apply radon transform to get max rotations
    rotations = [None, None, None, None]
    for i, band in enumerate("griz"):
        # Check if there is any data in this channel
        if not data[i].any():
            continue

        # Perform radon transform
        sinogram = radon_transform(data[i] * MASK)

        # Update rotation array
        rotation = np.unravel_index(sinogram.argmax(), sinogram.shape)[1]
        rotations[i] = rotation

    # Return processed rotations
    return [source_id, *rotations]


def run_script():
    iteration = 1
    while True:
        print(f"Processing batch #{iteration}...")
        with postgres() as cursor:
            cursor: extensions.cursor

            # Fetch batch from 'galaxies' table
            cursor.execute(f"""
                SELECT source_id, bin FROM galaxies
                WHERE status='Fetched'
                ORDER BY id LIMIT {SQL_BATCH_SIZE} FOR UPDATE SKIP LOCKED
            """)
            metadata = cursor.fetchall()

            # If we've completed processing of all fetched galaxies, stop loop
            if not metadata:
                break

            # Loop through every fetched galaxies
            result_rows = []
            for entry in tqdm(metadata):
                result_rows.append(process(entry))

            # Insert into the database tables
            # 1. fits_data table
            value_str = ",".join(cursor.mogrify("(%s,%s,%s,%s,%s)", row).decode("utf-8") for row in result_rows)
            cursor.execute(f"""
                INSERT INTO fits_data (source_id, rotation_g, rotation_r, rotation_i, rotation_z)
                VALUES {value_str};
            """)

            # 2. update 'galaxies' table's status
            cursor.execute("""
                UPDATE galaxies
                SET status = 'Transformed'
                WHERE source_id IN %s;
            """, (tuple(a[0] for a in result_rows),))

        # Call API to update status
        requests.post("http://backend:5000/pipeline/status/radon", json={
            "iteration": iteration,
            "galaxies": [a[0] for a in result_rows]
        })

        # Minor sleep
        sleep(1)
        iteration += 1
