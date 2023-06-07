import os
from time import sleep

import requests
from psycopg2 import extensions
from psycopg2 import sql
from requests.exceptions import Timeout
from tqdm import tqdm

from sql_util import postgres

# Fetch environment variables or use defaults
DATA_PATH = os.getenv("DATA_PATH", "/fits-data")
print(f"Configured environment variable DATA_PATH as '{DATA_PATH}'")

MAX_FAILS = int(os.getenv("MAX_FAILS", 1))
print(f"Configured environment variable MAX_FAILS as {MAX_FAILS}")

SQL_BATCH_SIZE = int(os.getenv("SQL_BATCH_SIZE", 200))
print(f"Configured environment variable SQL_BATCH_SIZE as {SQL_BATCH_SIZE}")


def build_url(ra: float, dec: float, size: int = 40, pix_scale: float = 0.262, bands: str = "griz"):
    """ Builds a URL to fetch a FITS file from the Legacy Survey API DR-10 """
    return f"https://www.legacysurvey.org/viewer/fits-cutout?ra={ra}&dec={dec}&layer=ls-dr10&size={size}&pixscale={pix_scale}&bands={bands}"


def fetch(url: str, fits_path: str) -> bool:
    """ Fetches a FITS file from the given URL and writes it to the given file path """
    try:
        with requests.get(url, allow_redirects=True, timeout=10) as r:
            status = r.status_code
            if status != 200:
                return False

            # Write to the file path
            os.makedirs(os.path.dirname(fits_path), exist_ok=True)
            with open(fits_path, "wb") as f:
                f.write(r.content)

        # Return successful
        return True
    except Timeout:
        return False


def process(row):
    """ Attempts to fetch the FITS file for the given galaxy row """
    uid, source_id, ra, dec, gal_prob, bin, status, failed_attempts = row

    # Build URL & fetch from API
    url = build_url(ra, dec)
    file_path = f"{DATA_PATH}/b{bin}/{source_id}.fits"
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


def run_script():
    iteration = 0
    while True:
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

            # If we've completed fetching of all galaxies, stop loop
            if not results:
                break

            # Process the rows in the fetched entries
            for row in (pbar := tqdm(results)):
                uid, status, failed_attempts = process(row)
                pbar.set_description(f"G{uid} - {status}")
                processed_results[uid] = (status, failed_attempts)
                sleep(0.5)

            values_str = ",".join(
                cursor.mogrify("(%s, %s, %s)", (id, status, failed_attempts)).decode("utf-8") for id, (status, failed_attempts) in processed_results.items())
            update_query = f"""
                UPDATE galaxies
                SET status = data.status,
                    failed_attempts = data.failed_attempts
                FROM (VALUES {values_str}) AS data (id, status, failed_attempts)
                WHERE galaxies.id = data.id
            """

            # Write final metadata to database
            cursor.execute(update_query)

        # Call API to update status
        galaxy_ids, successes, fails = [], [], []
        for galaxy_id, (status, failed_attempts) in processed_results.items():
            galaxy_ids.append(galaxy_id)
            (successes if status == "Fetched" else fails).append(galaxy_id)

        requests.post("http://backend:5000/pipeline/status/fetch", json={
            "iteration": iteration,
            "galaxies": galaxy_ids,
            "successes": successes,
            "fails": fails
        })

        print(f"Iteration #{iteration} ended with {len(successes)} successes and {len(fails)} fails (total: {len(galaxy_ids)})!")
        iteration += 1

        # Minor sleep after closing cursor
        sleep(5)
