import sys
from decimal import Decimal
from time import sleep
from typing import List, Tuple, Any, Dict

import requests
from psycopg2 import extensions
from requests import Response
from tqdm import tqdm

from commons.models.fits_interfaces import AbstractFitsInterface
from commons.orchestration.pipeline import AbstractScript
from commons.utils.parallel_utils import run_in_parallel
from commons.utils.sql_utils import AbstractPostgresClientFactory, PostgresClient
from constants import MAX_FAILS, SQL_BATCH_SIZE, CONTAINER_ID


class FetchScript(AbstractScript):
    def __init__(self, postgres_factory: AbstractPostgresClientFactory, fits_interface: AbstractFitsInterface):
        super().__init__()

        self.postgres_client: PostgresClient = postgres_factory.create()
        self.fits_interface: AbstractFitsInterface = fits_interface

        # Cache of the last batch of galaxies fetched
        # - format: [(status, failed_attempts, source_id), ...]
        self.status_cache: List[Tuple[str, int, str]] = []

        # Cache of the current iteration's progress
        # - max progress is SQL_BATCH_SIZE
        self.iteration_progress: int = 0

    def run_batch(self):
        print(f"Starting iteration #{self.iteration}...")
        self.iteration_progress = 0

        # Query and lock a batch of galaxies
        with self.postgres_client.cursor() as cursor:
            cursor: extensions.cursor
            cursor.execute("""
                SELECT source_id, ra, dec, bin_id, failed_attempts
                FROM galaxies
                WHERE gal_prob=1 
                    AND (status='Pending' OR (status='Failed' AND failed_attempts < %s))
                ORDER BY id
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            """, (MAX_FAILS, SQL_BATCH_SIZE))
            results: List[Tuple[str, Decimal, Decimal, int, int]] = cursor.fetchall()

        # If we've completed fetching of all galaxies, stop loop
        if not results:
            print("No more galaxies to fetch, stopping fetch script...")
            self.schedule_stop()
            return

        # Process in parallel with 5 threads
        def update_progress(pbar: tqdm):
            pbar.update()
            self.iteration_progress += 1

        with tqdm(range(len(results))) as pbar:
            fetch_results: List[Tuple[str, bool, int]] = run_in_parallel(self.parallel_fetch, results, thread_count=4, update_callback=update_progress, pbar=pbar)

        # Transform results into a format that can be used in the SQL query
        sql_friendly_results: List[Tuple[str, int, str]] = []
        success_count: int = 0
        for source_id, success, failed_attempts in fetch_results:
            status: str = "Fetched" if success else "Failed"
            sql_friendly_results.append((status, failed_attempts, source_id))

            if success:
                success_count += 1

        # Update database
        with self.postgres_client.cursor() as cursor:
            cursor: extensions.cursor
            cursor.executemany("""
                UPDATE galaxies
                SET status = %s,
                    failed_attempts = %s
                WHERE source_id = %s
            """, sql_friendly_results)

        # Update the status cache
        self.status_cache = sql_friendly_results

        print(f"Iteration #{self.iteration} complete with {success_count}/{len(results)} galaxies fetched successfully")

    def parallel_fetch(self, source_id: str, ra: Decimal, dec: Decimal, bin_id: int, failed_attempts: int) -> Tuple[str, bool, int]:
        """
        Fetches a FITS file for a single galaxy and saves it to disk

        Args:
            source_id (str): galaxy source ID
            ra (Decimal): galaxy right ascension
            dec (Decimal): galaxy declination
            bin_id (int): galaxy bin ID
            failed_attempts (int): current number of fails

        Returns:
            Tuple[str, bool, int]: galaxy source ID, success status, updated number of fails
        """
        sleep(0.8)  # respect the API rate limit

        # Fetch & save the FITS file
        try:
            url: str = self.build_url(float(ra), float(dec))
            with requests.get(url, allow_redirects=True, timeout=10) as response:
                response: Response
                if response.status_code != 200:
                    raise Exception(f"Request failed with status code {response.status_code}")
                raw_fits: bytes = response.content
        except Exception as e:
            print(f"Exception occurred while fetching FITS file for galaxy b{bin_id}/{source_id}: {e}", file=sys.stderr)
            return source_id, False, failed_attempts + 1

        # Write FITS data to disk
        try:
            self.fits_interface.save_fits(source_id, str(bin_id), raw_fits)
        except Exception as e:
            print(f"Exception occurred while saving FITS file for galaxy b{bin_id}/{source_id}: {e}", file=sys.stderr)
            return source_id, False, failed_attempts + 1

        return source_id, True, failed_attempts

    @staticmethod
    def build_url(ra: float, dec: float, size: int = 40, pix_scale: float = 0.262, bands: str = "griz"):
        """ Builds a URL to fetch a FITS file from the Legacy Survey API DR-10 """
        return f"https://www.legacysurvey.org/viewer/fits-cutout?ra={ra}&dec={dec}&layer=ls-dr10&size={size}&pixscale={pix_scale}&bands={bands}"

    def update_batch_status(self):
        print(f"Updating status for iteration #{self.iteration}...")
        try:
            requests.post(f"http://orchestrator:5000/pipelines/status/{CONTAINER_ID}", json={
                "iteration": self.iteration,
                "processed": self.status_cache
            })
        except Exception as e:
            print(f"Failed to update pipeline status to backend: {e}", file=sys.stderr)

    def get_status(self) -> Dict[str, Any]:
        return {
            "iteration": self.iteration,
            "iteration_progress": self.iteration_progress,
            "iteration_max_progress": SQL_BATCH_SIZE
        }
