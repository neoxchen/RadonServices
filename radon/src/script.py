import os
from time import sleep
from typing import Tuple, List

import numpy as np
import requests
import scipy
from astropy.io import fits
from psycopg2 import extensions
from scipy.optimize import curve_fit

from models import SineApproximation, NormalDistribution, RadonBandResult, RadonGalaxyResult
from sql_util import postgres

# Fetch environment variables or use defaults
CONTAINER_ID = os.getenv("CONTAINER_ID")
print(f"Configured environment variable CONTAINER_ID as '{CONTAINER_ID}'")

DATA_PATH = os.getenv("DATA_PATH", "/fits-data")
print(f"Configured environment variable DATA_PATH as '{DATA_PATH}'")

SQL_BATCH_SIZE = int(os.getenv("SQL_BATCH_SIZE", 200))
print(f"Configured environment variable SQL_BATCH_SIZE as {SQL_BATCH_SIZE}")

# We ignore the band data if all values are below this threshold
IGNORE_THRESHOLD = 0.00001

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

# Normal distribution fitting variables
DISTRIBUTION_WINDOWS = [15, 30, 60, 90]

# Dynamic flag to stop the script
stop_script = False


def set_stop_script():
    global stop_script
    stop_script = True


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


def filter_mean(x, threshold):
    index = int((1 - threshold) * x.shape[0])
    return np.mean(np.sort(x, axis=0)[index:], axis=0)


def filter_sinogram(sinogram):
    """ Returns a dictionary of filtered sinograms, with the key being the filter level """
    return {
        0: np.max(sinogram, axis=0),
        0.05: filter_mean(sinogram, 0.05),
        0.1: filter_mean(sinogram, 0.1),
        0.2: filter_mean(sinogram, 0.2),
        0.3: filter_mean(sinogram, 0.3)
    }


def expand_data(sinogram_1d: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """ Expands 1D sinogram data from [0, 180] to [-180, 180 * 2] degrees (wraps) """
    extended_data = np.concatenate((sinogram_1d[:-1], sinogram_1d, sinogram_1d[1:]))
    n = len(sinogram_1d)
    return np.arange(-(n - 1), (n - 1) * 2 + 1), extended_data


def sine_function(x, a, b, c, d):
    return a * np.sin(b * x + c) + d


def fit_sine(xs, ys):
    ff = np.fft.fftfreq(len(xs), (xs[1] - xs[0]))  # assume uniform spacing
    Fyy = abs(np.fft.fft(ys))
    guess_freq = abs(ff[np.argmax(Fyy[1:]) + 1])  # excluding the zero frequency "peak", which is related to offset
    guess_amp = np.std(ys) * 2.0 ** 0.5
    guess_offset = np.mean(ys)
    guess = np.array([guess_amp, 2.0 * np.pi * guess_freq, 0.0, guess_offset])
    return curve_fit(sine_function, xs, ys, p0=guess)


def process(metadata_entry) -> RadonGalaxyResult:
    source_id, bin_id = metadata_entry
    print(f"Processing galaxy b{bin_id}/{source_id}...")

    # Fetch FITS file from disk
    fits_path = f"{DATA_PATH}/b{bin_id}/{source_id}.fits"
    with fits.open(fits_path) as hdu_list:
        data = hdu_list[0].data
        hdu_list.close()

    # Apply radon transform to get max rotations for each band
    band_results: List[RadonBandResult] = []
    for i, band in enumerate("griz"):
        print(f"Processing band {band}...")

        # Check if there is any data (threshold) in this channel
        # - or if there are any NadNs, we want to ignore NaNs because it can propagate to the sinogram
        if not data[i].any():
            print("WARNING: No band data exists, skipping")
            continue
        elif np.any(np.isnan(data[i])):
            print("WARNING: Band data contains NaNs, skipping")
            continue

        # Perform radon transform
        sinogram = radon_transform(data[i] * MASK)
        if not sinogram.any():
            print("WARNING: No sinogram data exists, skipping")
            continue
        elif np.any(np.isnan(sinogram)):
            print("WARNING: Sinogram data contains NaNs, skipping")
            continue

        # Compute rotations & errors
        offset, rotation = np.unravel_index(sinogram.argmax(), sinogram.shape)

        # Apply filters to sinogram
        filtered_sinogram_map = filter_sinogram(sinogram)

        # Calculate sine approximation at each filter level
        xs = range(sinogram.shape[1])
        sine_approximations: List[SineApproximation] = []
        for level, sinogram_1d in filtered_sinogram_map.items():
            try:
                expanded_xs, expanded_ys = expand_data(sinogram_1d)
                popt, _ = fit_sine(expanded_xs, expanded_ys)
                sin_ys = sine_function(xs, *popt)
                rmse = np.sqrt(np.mean((sinogram_1d - sin_ys) ** 2))
                sine_approximations.append(SineApproximation(level, *popt, rmse))
            except:
                print(f"WARNING: Failed to fit sine approximation for level {level}, skipping")

        # Fit a normal distribution to each window
        normal_distributions: List[NormalDistribution] = []
        expanded_xs, expanded_ys = expand_data(filtered_sinogram_map[0])  # 0 = max filter
        for window in DISTRIBUTION_WINDOWS:
            # Mask the data to only include the window
            peak_mask = (expanded_xs >= rotation - window) & (expanded_xs <= rotation + window)
            xs_near_peak, ys_near_peak = expanded_xs[peak_mask], expanded_ys[peak_mask]

            # Normalize the intensity
            ys_near_peak_normalized = ys_near_peak / np.sum(ys_near_peak)

            # Calculated weighted mean and standard deviation
            mean = np.average(xs_near_peak, weights=ys_near_peak_normalized)
            std = np.sqrt(np.average((xs_near_peak - mean) ** 2, weights=ys_near_peak_normalized))

            # Check NaNs
            if np.isnan(mean) or np.isnan(std):
                print(f"WARNING: Normal distribution mean or std is NaN, skipping window {window}")
            else:
                # Append to list
                normal_distributions.append(NormalDistribution(window, mean, std))

        band_results.append(RadonBandResult(band, rotation, sine_approximations, normal_distributions))

    return RadonGalaxyResult(source_id, bin_id, band_results)


def insert_result(cursor: extensions.cursor, result: RadonGalaxyResult) -> None:
    # Assuming cursor is managed with a with statement
    for band_result in result.band_results:
        # Insert into bands and get band id
        cursor.execute(
            "INSERT INTO bands (source_id, band, degree) VALUES (%s, %s, %s) RETURNING id",
            (result.source_id, band_result.band_name, band_result.degree)
        )
        band_id = cursor.fetchone()[0]

        # Insert into sine_approximations
        for sine_approximation in band_result.sine_approximations:
            cursor.execute(
                "INSERT INTO sine_approximations (band_id, level, a, b, c, d, rmse) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (band_id, sine_approximation.level, sine_approximation.a, sine_approximation.b, sine_approximation.c, sine_approximation.d,
                 sine_approximation.rmse)
            )

        # Insert into normal_distributions
        for normal_distribution in band_result.normal_distributions:
            cursor.execute(
                "INSERT INTO normal_distributions (band_id, window_size, mean, std_dev) VALUES (%s, %s, %s, %s)",
                (band_id, normal_distribution.window, normal_distribution.mean, normal_distribution.std_dev)
            )


def run_script():
    print("Starting radon pipeline script...")
    iteration = 1
    while not stop_script:
        # Minor sleep
        sleep(2)

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
            result_rows: List[RadonGalaxyResult] = []
            for galaxy_entry in metadata:
                try:
                    galaxy_result = process(galaxy_entry)
                except:
                    print(f"ERROR: Failed to process galaxy b{galaxy_entry[1]}/{galaxy_entry[0]}")
                    galaxy_result = RadonGalaxyResult(galaxy_entry[0], galaxy_entry[1], [], is_error=True)
                result_rows.append(galaxy_result)

            # Insert into the database tables
            # 1. insert into the various results tables
            for result in result_rows:
                if not result.is_error:
                    insert_result(cursor, result)

            # 2. update 'galaxies' table's status
            # Update transformed galaxies
            transformed_galaxies = tuple(result_entry.source_id for result_entry in result_rows if not result_entry.is_error)
            if transformed_galaxies:
                cursor.execute("""
                    UPDATE galaxies
                    SET status = 'Transformed'
                    WHERE source_id IN %s;
                """, (transformed_galaxies,))

            # Update error galaxies
            error_galaxies = tuple(result_entry.source_id for result_entry in result_rows if result_entry.is_error)
            if error_galaxies:
                cursor.execute("""
                    UPDATE galaxies
                    SET status = 'Error'
                    WHERE source_id IN %s;
                """, (error_galaxies,))

        # Call API to update status
        print(f"Updating pipeline status for iteration #{iteration}...")
        requests.post(f"http://backend:5000/pipelines/status/{CONTAINER_ID}", json={
            "iteration": iteration,
            "galaxies": [result_entry.source_id for result_entry in result_rows if not result_entry.is_error],
            "errors": [result_entry.source_id for result_entry in result_rows if result_entry.is_error]
        })

        print(f"Iteration #{iteration} ended with {len(result_rows)} galaxies processed")
        iteration += 1

    # Signal pipeline shutdown
    requests.delete(f"http://backend:5000/pipelines/status/{CONTAINER_ID}")
    print("Radon pipeline script execution complete!")
