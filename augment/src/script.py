import sys
from typing import Any, Dict, Tuple, Optional, List

import numpy as np
from psycopg2 import extensions
from tqdm import tqdm

from commons.constants.fits_constants import FITS_BANDS
from commons.models.augmenters import AbstractAugmenter, AugmentResult
from commons.models.denoisers import AbstractDenoiser
from commons.models.fits_interfaces import AbstractBatchFilePathGenerator, GalaxyFitsData, BatchFileReader, BatchFitsReader
from commons.models.mask_generators import AbstractMaskGenerator
from commons.models.radon_transformers import RadonTransformer
from commons.models.running_error import RunningErrorCalculator
from commons.orchestration.pipeline import AbstractScript
from commons.utils.parallel_utils import run_in_parallel
from commons.utils.sql_utils import AbstractPostgresClientFactory, PostgresClient
from constants import THREAD_COUNT, AUGMENTATION_COUNT


class AugmentScript(AbstractScript):
    def __init__(self, postgres_factory: AbstractPostgresClientFactory,
                 batch_path_generator: AbstractBatchFilePathGenerator,
                 augmenter: AbstractAugmenter,
                 radon_transformer: RadonTransformer,
                 mask_generator: AbstractMaskGenerator,
                 denoiser: AbstractDenoiser):
        super().__init__()

        self.postgres_client: PostgresClient = postgres_factory.create()
        self.batch_path_generator: AbstractBatchFilePathGenerator = batch_path_generator
        self.augmenter: AbstractAugmenter = augmenter
        self.radon_transformer: RadonTransformer = radon_transformer
        self.mask_generator: AbstractMaskGenerator = mask_generator
        self.denoiser: AbstractDenoiser = denoiser

    def run_batch(self):
        print(f"Starting iteration #{self.iteration}...")

        # We want to keep all processing in a single transaction for concurrency control
        with self.postgres_client.cursor() as cursor:
            # Query to fetch the next batch of galaxies to process
            # 1. Fetch the first batch_id from eligible_bands (aka unprocessed galaxies)
            # 2. Fetch all bands with the same batch_id, locking them for processing
            cursor.execute(f"""
                SELECT uid, source_id, bin_id, batch_id, fits_index
                FROM augment_view
                WHERE batch_id = (
                    SELECT batch_id
                    FROM augment_view
                    LIMIT 1
                )
                ORDER BY source_id, fits_index
                FOR UPDATE SKIP LOCKED;
            """)
            metadata_list: List[Tuple[int, str, str, str, int]] = cursor.fetchall()

            # If we've completed processing of all fetched galaxies, stop loop
            if not metadata_list:
                print("No more galaxies to process, stopping script!")
                self.schedule_stop()
                return

            # Locate the .batch file for the current batch
            bin_id: str = metadata_list[0][2]
            batch_id: str = metadata_list[0][3]
            batch_path: str = self.batch_path_generator.generate(bin_id, batch_id)

            # Load the batch file
            batch_file_reader: BatchFileReader = BatchFileReader.from_file(batch_path)
            with batch_file_reader.decompress() as batch_fits_reader:
                batch_fits_reader: BatchFitsReader

                def to_args(metadata: Tuple[int, str, str, str, int]) -> Tuple[BatchFitsReader, int, str, int]:
                    """ Converts each metadata tuple into arguments for parallel processing """
                    uid, source_id, bin_id, batch_id, fits_index = metadata
                    return batch_fits_reader, uid, source_id, fits_index

                # Process in parallel with THREAD_COUNT threads
                pbar = tqdm(metadata_list, desc=f"Iteration #{self.iteration}", total=len(metadata_list))
                process_results: List[Tuple[int, Optional[RunningErrorCalculator], bool]] = run_in_parallel(
                    self.parallel_process,
                    [to_args(metadata) for metadata in metadata_list],
                    THREAD_COUNT,
                    lambda: pbar.update(1)
                )
                pbar.close()

            # Insert results into database
            successful, failed = 0, 0
            for metadata, process_result in zip(metadata_list, process_results):
                band_uid, error_calculator, is_error = process_result
                if not is_error:
                    self.insert_result(cursor, band_uid, error_calculator)
                    successful += 1
                else:
                    self.insert_error(cursor, band_uid)
                    failed += 1

        print(f"Processed {successful} galaxies successfully, {failed} failed, total {len(metadata_list)} galaxies")

    def parallel_process(self, batch_fits_reader: BatchFitsReader, band_uid: int, source_id: str, fits_index: int) -> Tuple[int, Optional[RunningErrorCalculator], bool]:
        """ Processes a single galaxy band, can be run in parallel """
        band: str = FITS_BANDS[fits_index]

        try:
            # Load FITS data
            band_fits_data_container: GalaxyFitsData = batch_fits_reader.get_fits(source_id)
            if band_fits_data_container is None or band_fits_data_container.get_band_data(band) is None:
                raise Exception(f"FITS data for {source_id} band #{fits_index} is invalid, unable to load & preprocess")
            band_fits_array: np.ndarray = band_fits_data_container.get_band_data(band).mask(self.mask_generator).denoise(self.denoiser).build()

            # Calculate "oracle" rotation
            ideal_rotation: float = self.radon_transformer.transform(band_fits_array).get_rotation()

            # Generate augmentations & estimate error
            augmentations: List[AugmentResult] = self.augmenter.random_augment(band_fits_array, count=AUGMENTATION_COUNT)
            running_error_calculator: RunningErrorCalculator = RunningErrorCalculator()

            for augment_result in augmentations:
                augment_result: AugmentResult
                actual_rotation: float = self.radon_transformer.transform(augment_result.result_image).get_rotation()
                running_error_calculator.update(int(ideal_rotation + augment_result.angle_delta), int(actual_rotation))

            return band_uid, running_error_calculator, False
        except Exception as e:
            print(f"Error processing source_id={source_id}, band={band}", file=sys.stderr)
            print(e, file=sys.stderr)

            # Set error flag
            return band_uid, None, True

    @staticmethod
    def insert_result(cursor: extensions.cursor, band_uid: int, error: RunningErrorCalculator) -> None:
        cursor.execute("""
                UPDATE rotations
                SET total_error = total_error + %s,
                    running_count = running_count + %s
                WHERE band_uid = %s
            """, (error.total_error, error.running_count, band_uid))

    @staticmethod
    def insert_error(cursor: extensions.cursor, band_uid: int) -> None:
        cursor.execute("""
                UPDATE bands
                SET has_error = TRUE
                WHERE uid = %s
            """, (band_uid,))

    def update_batch_status(self):
        # Do nothing
        pass

    def get_status(self) -> Dict[str, Any]:
        return {
            "iteration": self.iteration,
            "iteration_progress": 0,
            "iteration_max_progress": THREAD_COUNT
        }
