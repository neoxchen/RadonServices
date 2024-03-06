import os
from time import sleep

import requests
from psycopg2 import extensions
from psycopg2 import sql
from tqdm import tqdm

from commons.utils.parallel_utils import run_in_parallel
from constants import CONTAINER_ID, MAX_FAILS, SQL_BATCH_SIZE

# Dynamic flag to stop the script
stop_fetch = False


def set_stop_fetch():
    global stop_fetch
    stop_fetch = True


def build_url(ra: float, dec: float, size: int = 40, pix_scale: float = 0.262, bands: str = "griz"):
    """ Builds a URL to fetch a FITS file from the Legacy Survey API DR-10 """
    return f"https://www.legacysurvey.org/viewer/fits-cutout?ra={ra}&dec={dec}&layer=ls-dr10&size={size}&pixscale={pix_scale}&bands={bands}"


def process(row):
    """ Attempts to fetch the FITS file for the given galaxy row """
    sleep(0.8)
    uid, source_id, ra, dec, gal_prob, bin_id, status, failed_attempts = row

    # Build URL & fetch from API
    url = build_url(ra, dec)
    file_path = f"{DATA_PATH}/b{bin_id}/{source_id}.fits"
    successful = fetch(url, file_path)

    # Decide what to do based on the fetch results
    if successful:
        # Write FITS metadata to the database
        status = "Fetched"
    else:
        # Update fail count
        failed_attempts += 1

        # Check for max fails
        if failed_attempts >= MAX_FAILS:
            status = "Failed"

    return uid, status, failed_attempts


def fetch(url: str, fits_path: str) -> bool:
    """ Fetches a FITS file from the given URL and writes it to the given file path """
    # print(f"Fetching {url} to {fits_path}...")
    try:
        with requests.get(url, allow_redirects=True, timeout=10) as r:
            status = r.status_code
            if status != 200:
                # print(f"Request failed with status code {status}!")
                # TODO: add 503 service unavailable check
                return False

            # Write to the file path
            os.makedirs(os.path.dirname(fits_path), exist_ok=True)
            with open(fits_path, "wb") as f:
                f.write(r.content)

        # Return successful
        # print("Successfully fetched!")
        return True
    except:
        # print("Exception occurred while fetching!")
        return False


def run_script():
    print("Starting fetching pipeline script...")
    iteration = 0
    while not stop_fetch:
        # Minor sleep before each iteration
        sleep(2)

        print(f"Iteration #{iteration} started...")
        processed_results = {}
        with postgres() as cursor:
            # Add type hint
            cursor: extensions.cursor

            # Query & lock a batch of rows
            query = sql.SQL(f"""
                SELECT * FROM galaxies
                WHERE status='Pending' AND gal_prob=1
                ORDER BY id LIMIT {SQL_BATCH_SIZE} FOR UPDATE SKIP LOCKED
            """)
            cursor.execute(query)
            results = cursor.fetchall()
            print(f"Fetched batch of {len(results)} galaxies from database")

            # If we've completed fetching of all galaxies, stop loop
            if not results:
                print("No more galaxies to fetch, stopping fetch script...")
                break

            # Process in parallel with 5 threads
            with tqdm(range(len(results))) as pbar:
                processed = run_in_parallel(process, [[row] for row in results], thread_count=5, update_callback=lambda: pbar.update())
            for uid, status, failed_attempts in processed:
                processed_results[uid] = (status, failed_attempts)

            # Process in serial
            # for i, row in enumerate(results):
            #     print(f"Processing galaxy #{row[0]} ({i + 1}/{len(results)})...")
            #     uid, status, failed_attempts = process(row)
            #     processed_results[uid] = (status, failed_attempts)
            #     sleep(1)

            # Update database
            values_str = ",".join(
                cursor.mogrify("(%s, %s, %s)", (gid, status, failed_attempts)).decode("utf-8") for gid, (status, failed_attempts) in processed_results.items())
            cursor.execute(f"""
                UPDATE galaxies
                SET status = data.status,
                    failed_attempts = data.failed_attempts
                FROM (VALUES {values_str}) AS data (id, status, failed_attempts)
                WHERE galaxies.id = data.id
            """)

        # Call API to update status
        galaxy_ids, successes, fails = [], [], []
        for galaxy_id, (status, failed_attempts) in processed_results.items():
            galaxy_ids.append(galaxy_id)
            (successes if status == "Fetched" else fails).append(galaxy_id)

        print(f"Updating pipeline status for iteration #{iteration}...")
        requests.post(f"http://backend:5000/pipelines/status/{CONTAINER_ID}", json={
            "iteration": iteration,
            "galaxies": galaxy_ids,
            "successes": successes,
            "fails": fails
        })

        print(
            f"Iteration #{iteration} ended with {len(successes)} successes and {len(fails)} fails (total: {len(galaxy_ids)})!")
        iteration += 1

    # Signal pipeline shutdown
    requests.delete(f"http://backend:5000/pipelines/status/{CONTAINER_ID}")

    print("Fetching pipeline script execution complete!")
