import random
import sys
from typing import List, Tuple, Dict, Any

import numpy as np
import skimage
from astropy.io import fits
from psycopg2 import sql

from commons.constants.fits_constants import FITS_DIRECTORY_PATH
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

    def __init__(self, client_factory: AbstractPostgresClientFactory):
        self.client: PostgresClient = client_factory.create()

    def supply(self, count: int = 1) -> List[DataSupplyResult]:
        # Fetch twice as many galaxies as needed to account for any that may be unusable
        fetch_count = count * 2
        with self.client.cursor() as cursor:
            query = sql.SQL(f"""
                SELECT g.source_id, g.bin_id
                FROM galaxies g
                WHERE g.status NOT IN ('Pending', 'Failed')
                    AND g.gal_prob = 1
                LIMIT {fetch_count}
            """)
            cursor.execute(query)

            galaxy_data_list = []
            i = 0
            while len(galaxy_data_list) < count and i < fetch_count:
                try:
                    source_id, bin_id = cursor.fetchone()
                    galaxy_data = self._fetch_galaxy_fits(source_id, bin_id)
                    if galaxy_data is not None:
                        galaxy_data_result = DataSupplyResult(galaxy_data, {"source_id": source_id, "bin_id": bin_id})
                        galaxy_data_list.append(galaxy_data_result)
                except Exception as e:
                    print(f"Error fetching galaxy ({len(galaxy_data_list)}/{count}): {e}", file=sys.stderr)
                finally:
                    i += 1

        if len(galaxy_data_list) < count:
            raise DataSupplyError(f"Failed to supply {count} galaxy data, only supplied {len(galaxy_data_list)}")

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
    postgres_client_factory = LocalPostgresClientFactory()

    # Preview generators
    temp_count = 5
    temp_data_supply = EllipseDataSupplier((8, 18), (5, 10)).supply(temp_count)
    temp_data_supply += GalaxyDataSupplier(postgres_client_factory).supply(temp_count)

    fig, ax = plt.subplots(2, 5, figsize=(10, 4))
    for i in range(temp_count * 2):
        ax[i // 5, i % 5].imshow(temp_data_supply[i].data, cmap="gray")
        ax[i // 5, i % 5].axis('off')

    plt.tight_layout()
    plt.show()
