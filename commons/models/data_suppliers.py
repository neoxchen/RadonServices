import random
from typing import List, Tuple, Dict, Any, Optional

import numpy as np
import skimage
from astropy.io import fits
from psycopg2 import sql

from commons.constants.fits_constants import FITS_DIRECTORY_PATH, BATCH_FITS_SIZE, FITS_BANDS
from commons.models.fits_interfaces import AbstractBatchFilePathGenerator, BatchFitsReader, GalaxyFitsData, BandFitsBuilder, LocalTestingBatchFilePathGenerator
from commons.models.sql_models import SqlBand
from commons.utils.sql_utils import AbstractPostgresClientFactory, PostgresClient


class DataSupplyResult:
    def __init__(self, data: np.ndarray, metadata: Dict[str, Any]):
        self.data = data
        self.metadata = metadata


class DataSupplyError(Exception):
    pass


class AbstractDataSupplier:
    def supply(self, count: int = 1) -> List[DataSupplyResult]:
        raise NotImplementedError


class EllipseDataSupplier(AbstractDataSupplier):
    """ Supplies synthetic ellipse data """

    def __init__(self, radius_major_bounds: Tuple[int, int], radius_minor_bounds: Tuple[int, int], shape=(40, 40),
                 blur_strength=2, noise_intensity=0.03, random_seed=42):
        self.radius_major_bounds = radius_major_bounds
        self.radius_minor_bounds = radius_minor_bounds

        self.shape = shape
        self.center = (self.shape[0] // 2, self.shape[1] // 2)

        self.blur_strength = blur_strength
        self.noise_intensity = noise_intensity

        random.seed(random_seed)

    def supply(self, count: int = 1) -> List[DataSupplyResult]:
        return [self._supply_one(*self.random_ellipse_params(), random.randint(0, 360)) for _ in range(count)]

    def supply_fixed(self, major: float, minor: float, rotation: int) -> DataSupplyResult:
        return self._supply_one(major, minor, rotation)

    def _supply_one(self, major: float, minor: float, rotation: int) -> DataSupplyResult:
        canvas = np.zeros(self.shape, dtype=np.uint8)

        # Generate the coordinates of the ellipse
        rr, cc = skimage.draw.ellipse(*self.center, major, minor, self.shape, rotation)
        # Set the pixels of the ellipse to white
        canvas[rr, cc] = 1

        # Blur & normalize
        canvas = skimage.filters.gaussian(canvas, sigma=self.blur_strength)
        canvas = canvas / np.max(canvas)

        # Generate noise
        noise = np.random.normal(0, self.noise_intensity, self.shape)
        canvas += noise

        # Shift to positive
        if np.min(canvas) < 0:
            canvas += np.abs(np.min(canvas))

        return DataSupplyResult(canvas, {"major": major, "minor": minor, "rotation": rotation})

    def random_ellipse_params(self):
        radius_major = random.uniform(*self.radius_major_bounds)
        radius_minor = random.uniform(*self.radius_minor_bounds)
        return radius_major, radius_minor


class GalaxyDataSupplier(AbstractDataSupplier):
    """ Fetches galaxy data from the database """

    def __init__(self, client_factory: AbstractPostgresClientFactory, batch_path_generator: AbstractBatchFilePathGenerator):
        self.client: PostgresClient = client_factory.create()
        self.batch_path_generator: AbstractBatchFilePathGenerator = batch_path_generator

    def supply(self, count: int = 1) -> List[DataSupplyResult]:
        if count > BATCH_FITS_SIZE:
            raise DataSupplyError(f"Cannot supply more than {BATCH_FITS_SIZE} galaxy data at once")

        # Query for a single galaxy, as we'll fetch the entire batch it's in
        with self.client.cursor() as cursor:
            query = sql.SQL(f"""
                SELECT * FROM bands
                WHERE has_error = FALSE
                LIMIT 1
            """)
            cursor.execute(query)
            results: List[SqlBand] = [SqlBand(*row) for row in cursor.fetchall()]

        if not results:
            raise DataSupplyError("No galaxy data available")

        batch_file_path: str = self.batch_path_generator.generate(results[0].bin_id, results[0].batch_id)
        reader: BatchFitsReader = BatchFitsReader.from_file(batch_file_path)

        galaxy_data_list: List[DataSupplyResult] = []

        current_supply_count: int = 0
        for galaxy_fits_data in reader.loop_fits():
            galaxy_fits_data: GalaxyFitsData
            if current_supply_count >= count:
                break

            for band in FITS_BANDS:
                band: str
                if current_supply_count >= count:
                    break

                band_data: Optional[BandFitsBuilder] = galaxy_fits_data.get_band_data(band)
                if not band_data:
                    continue
                galaxy_data_result: DataSupplyResult = DataSupplyResult(band_data.build(), {"source_id": galaxy_fits_data.source_id, "band": band})
                galaxy_data_list.append(galaxy_data_result)
                current_supply_count += 1

        return galaxy_data_list

    @staticmethod
    def _fetch_galaxy_fits(source_id: str, bin_id: str) -> np.ndarray:
        with fits.open(f"{FITS_DIRECTORY_PATH}/b{bin_id}/{source_id}.fits") as hdu_list:
            fits_data_list = hdu_list[0].data

        for i, (band_data, band_name) in enumerate(zip(fits_data_list, "griz")):
            if not band_data.any() or np.any(np.isnan(band_data)):
                continue
            # Shift FITS data to be non-negative
            shift = np.min(band_data)
            if shift < 0:
                band_data += abs(shift)
            # Normalize
            band_data /= np.max(band_data)
            return band_data

        raise ValueError(f"No usable data found for source_id {source_id}")


if __name__ == "__main__":
    import matplotlib.pyplot as plt
    from commons.utils.sql_utils import LocalPostgresClientFactory

    # Create local database client
    postgres_client_factory: AbstractPostgresClientFactory = LocalPostgresClientFactory()
    batch_path_generator: AbstractBatchFilePathGenerator = LocalTestingBatchFilePathGenerator()

    # Preview generators
    temp_count = 5
    temp_data_supply = EllipseDataSupplier((8, 18), (5, 10)).supply(temp_count)
    temp_data_supply += GalaxyDataSupplier(postgres_client_factory, batch_path_generator).supply(temp_count)

    fig, ax = plt.subplots(2, 5, figsize=(10, 4))
    for i in range(temp_count * 2):
        ax[i // 5, i % 5].imshow(temp_data_supply[i].data, cmap="gray")
        ax[i // 5, i % 5].axis('off')

    plt.tight_layout()
    plt.show()
