import random
from typing import List, Tuple, Dict, Any, Optional

import numpy as np
import skimage
from psycopg2 import sql
from scipy.ndimage import gaussian_filter

from commons.constants.fits_constants import BATCH_FITS_SIZE, FITS_BANDS
from commons.models.fits_interfaces import AbstractBatchFilePathGenerator, BatchFitsReader, GalaxyFitsData, BandFitsBuilder, LocalTestingBatchFilePathGenerator
from commons.models.image import AbstractImage, SingleChannelImage
from commons.models.sql_models import SqlBand
from commons.utils.sql_utils import AbstractPostgresClientFactory, PostgresClient


class DataEntry:
    def __init__(self, data: Any, metadata: Optional[Dict[str, Any]] = None):
        if metadata is None:
            metadata = {}

        self.data: Any = data
        self.metadata: Dict[str, Any] = metadata


class DataSupplyResult:
    def __init__(self, entries: List[DataEntry], metadata: Optional[Dict[str, Any]] = None):
        if metadata is None:
            metadata = {}

        self.entries: List[DataEntry] = entries
        self.metadata: Dict[str, Any] = metadata


class DataSupplyError(Exception):
    """ Placeholder class for data supply errors """
    pass


class AbstractDataSupplier:
    def supply(self, count: int = 1) -> DataSupplyResult:
        raise NotImplementedError

    def get_source_name(self) -> str:
        raise NotImplementedError

    def __repr__(self):
        raise NotImplementedError


class EllipseDataSupplier(AbstractDataSupplier):
    """ Supplies synthetic ellipse data """

    def __init__(self, radius_major_bounds: Tuple[int, int], radius_minor_bounds: Tuple[int, int], shape: Tuple[int, int] = (40, 40),
                 blur_strength: float = 2, noise_intensity: float = 0.03, random_seed: int = 42):
        self.radius_major_bounds: Tuple[int, int] = radius_major_bounds
        self.radius_minor_bounds: Tuple[int, int] = radius_minor_bounds

        self.shape: Tuple[int, int] = shape
        self.center: Tuple[int, int] = (self.shape[0] // 2, self.shape[1] // 2)

        self.blur_strength: float = blur_strength
        self.noise_intensity: float = noise_intensity

        random.seed(random_seed)

    def supply(self, count: int = 1) -> DataSupplyResult:
        entries: List[DataEntry] = [self._supply_one(*self.random_ellipse_params(), random.randint(0, 360)) for _ in range(count)]
        return DataSupplyResult(entries)

    def supply_fixed(self, major: float, minor: float, rotation: int) -> DataEntry:
        return self._supply_one(major, minor, rotation)

    def _supply_one(self, major: float, minor: float, rotation: int) -> DataEntry:
        canvas: np.ndarray = np.zeros(self.shape, dtype=np.uint8)

        # Generate the coordinates of the ellipse
        rr, cc = skimage.draw.ellipse(*self.center, major, minor, self.shape, rotation)
        # Set the pixels of the ellipse to white
        canvas[rr, cc] = 1

        # Blur & normalize
        canvas: np.ndarray = skimage.filters.gaussian(canvas, sigma=self.blur_strength)
        canvas: np.ndarray = canvas / np.max(canvas)

        # Generate noise
        noise: np.ndarray = np.random.normal(0, self.noise_intensity, self.shape)
        canvas += noise

        # Shift to positive
        if np.min(canvas) < 0:
            canvas += np.abs(np.min(canvas))

        return DataEntry(SingleChannelImage(canvas), {
            "major": major,
            "minor": minor,
            "rotation": rotation
        })

    def random_ellipse_params(self) -> Tuple[float, float]:
        radius_major: float = random.uniform(*self.radius_major_bounds)
        radius_minor: float = random.uniform(*self.radius_minor_bounds)
        return radius_major, radius_minor

    def get_source_name(self) -> str:
        return "Ellipse"

    def __repr__(self):
        return F"EllipseDataSupplier(major={self.radius_major_bounds}, minor={self.radius_minor_bounds})"


class GalaxyDataSupplier(AbstractDataSupplier):
    """ Fetches galaxy data from the database """

    def __init__(self, client_factory: AbstractPostgresClientFactory, batch_path_generator: AbstractBatchFilePathGenerator):
        self.client: PostgresClient = client_factory.create()
        self.batch_path_generator: AbstractBatchFilePathGenerator = batch_path_generator

    def supply(self, count: int = 1) -> DataSupplyResult:
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

        galaxy_data_list: List[DataEntry] = []
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
                galaxy_data_result: DataEntry = DataEntry(band_data.build(), {"source_id": galaxy_fits_data.source_id, "band": band})
                galaxy_data_list.append(galaxy_data_result)
                current_supply_count += 1

        return DataSupplyResult(galaxy_data_list)

    def get_source_name(self) -> str:
        return "Galaxy"

    def __repr__(self):
        return "GalaxyDataSupplier()"


class AbstractIntensityProfileDataSupplier(AbstractDataSupplier):
    """ Supplies synthetic galaxy data based on intensity profiles """

    def supply(self, count: int = 1, ellipticity: float = 0, rotation_degree: int = 0, psf_sigma: float = 1, noise: float = 0.003) -> DataSupplyResult:
        image_shape: Tuple[int, int] = (40, 40)
        return DataSupplyResult([self._supply_one(image_shape, ellipticity=ellipticity, rotation_degree=rotation_degree, psf_sigma=psf_sigma, noise=noise) for _ in range(count)])

    def _supply_one(self, image_shape: Tuple[int, int], ellipticity: float = 0, rotation_degree: int = 0, psf_sigma: float = 1, noise: float = 0.003) -> DataEntry:
        # Generate the canvas
        x, y = np.meshgrid(np.arange(image_shape[0]), np.arange(image_shape[1]))
        x: np.ndarray = x - image_shape[0] // 2
        y: np.ndarray = y - image_shape[1] // 2

        # Rotate the coordinates
        radians: float = np.deg2rad(rotation_degree)
        x_prime: np.ndarray = x * np.cos(radians) - y * np.sin(radians)
        y_prime: np.ndarray = x * np.sin(radians) + y * np.cos(radians)

        # Incorporate the ellipticity
        q: float = 1 - ellipticity
        radius: np.ndarray = np.sqrt(x_prime ** 2 + (y_prime / q) ** 2)

        # Calculate the distance from the center
        raw_image: np.ndarray = self._calculate_intensity(radius)

        # Normalize & clip
        raw_image = raw_image / np.max(raw_image)
        raw_image = np.clip(raw_image, 0, 1)

        # Simulate point spread function (PSF) via Gaussian filter
        raw_image = gaussian_filter(raw_image, sigma=psf_sigma)

        # Add Gaussian noise
        raw_image += np.random.normal(0, noise, raw_image.shape)
        raw_image = np.clip(raw_image, 0, 1)

        metadata: Dict[str, Any] = self._get_general_metadata()
        metadata.update({
            "ellipticity": ellipticity,
            "rotation_degree": rotation_degree,
            "psf_sigma": psf_sigma,
            "noise": noise
        })

        image: AbstractImage = SingleChannelImage(raw_image, metadata)
        return DataEntry(image, metadata)

    def _calculate_intensity(self, distance_from_center: np.ndarray) -> np.ndarray:
        raise NotImplementedError

    def _get_general_metadata(self) -> Dict[str, Any]:
        raise NotImplementedError


class SersicDataSupplier(AbstractIntensityProfileDataSupplier):
    """
    Uses the Sersic intensity profile to simulate galaxy images
    I(R) = I_e * exp(-b_n * ((R / R_e) ^ (1 / n) - 1))

    Where:
    - I(R) is the intensity at radius R
    - R_e is the effective (half-light) radius
    - I_e is the intensity at R_e
    - n is the Sersic index, where n = 1 is an exponential profile and n = 4 is a de Vaucouleurs profile
    - b_n is a constant that depends on n
    """

    def __init__(self, sersic_index: float, effective_intensity: float, effective_radius: float):
        self.sersic_index: float = sersic_index
        self.effective_intensity: float = effective_intensity
        self.effective_radius: float = effective_radius

    def _calculate_intensity(self, distance_from_center: np.ndarray) -> np.ndarray:
        b_n: float = self._calculate_b_n(self.sersic_index)
        return self.effective_intensity * np.exp(-b_n * ((distance_from_center / self.effective_radius) ** (1 / self.sersic_index) - 1))

    def _get_general_metadata(self) -> Dict[str, Any]:
        return {
            "effective_radius": self.effective_radius,
            "effective_intensity": self.effective_intensity,
            "sersic_index": self.sersic_index
        }

    @staticmethod
    def _calculate_b_n(n: float):
        """ Asymptotic form for b_n """
        return 2 * n - 0.324 + 4.0 / (405 * n) + 46.0 / (25515 * n ** 2) + 131.0 / (1148175 * n ** 3) - 2194697.0 / (30690717750 * n ** 4)

    def get_source_name(self) -> str:
        return F"SersicProfile[n={self.sersic_index}, I_e={self.effective_intensity}, R_e={self.effective_radius}]"

    def __repr__(self):
        return F"SersicDataSupplier(n={self.sersic_index}, I_e={self.effective_intensity}, R_e={self.effective_radius})"


class DeVaucouleursDataSupplier(SersicDataSupplier):
    """ Uses the de Vaucouleurs intensity profile to simulate galaxy images, a special case of the Sersic profile with n = 4 """

    def __init__(self, effective_intensity: float, effective_radius: float):
        super().__init__(4, effective_intensity, effective_radius)

    def get_source_name(self) -> str:
        return F"DeVaucouleursProfile[I_e={self.effective_intensity}, R_e={self.effective_radius}]"

    def __repr__(self):
        return F"DeVaucouleursDataSupplier(I_e={self.effective_intensity}, R_e={self.effective_radius})"


if __name__ == "__main__":
    import matplotlib.pyplot as plt
    from commons.utils.sql_utils import LocalPostgresClientFactory

    # Create local database client
    postgres_client_factory: AbstractPostgresClientFactory = LocalPostgresClientFactory()
    batch_path_generator: AbstractBatchFilePathGenerator = LocalTestingBatchFilePathGenerator()

    # Preview generators
    temp_count: int = 5
    temp_data_supply: List[DataEntry] = EllipseDataSupplier((8, 18), (5, 10)).supply(temp_count).entries
    temp_data_supply += GalaxyDataSupplier(postgres_client_factory, batch_path_generator).supply(temp_count).entries
    temp_data_supply += SersicDataSupplier(1, 10, 100).supply(temp_count, ellipticity=0.4).entries
    temp_data_supply += DeVaucouleursDataSupplier(10, 100).supply(temp_count, ellipticity=0.4).entries

    fig, ax = plt.subplots(4, 5, figsize=(10, 4))
    for i in range(len(temp_data_supply)):
        image: AbstractImage = temp_data_supply[i].data
        ax[i // 5, i % 5].imshow(image.get_image(), cmap="gray")
        ax[i // 5, i % 5].axis('off')

    plt.tight_layout()
    plt.show()
