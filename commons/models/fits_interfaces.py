import sys
from io import BytesIO
from typing import Dict, Optional, List, Generator

import numpy as np
from astropy.io import fits

from commons.constants.fits_constants import FITS_BANDS
from commons.models.denoisers import AbstractDenoiser
from commons.models.file_batcher import BatchFile, FileMetadata
from commons.models.mask_generators import AbstractMaskGenerator


class BandFitsBuilder:
    def __init__(self, fits_data: np.ndarray):
        self._fits_data: np.ndarray = fits_data

        self._should_preprocess: bool = True
        self._mask_generator: Optional[AbstractMaskGenerator] = None
        self._denoiser: Optional[AbstractDenoiser] = None

    def mask(self, mask_generator: AbstractMaskGenerator) -> "BandFitsBuilder":
        self._mask_generator = mask_generator
        return self

    def denoise(self, denoiser: AbstractDenoiser) -> "BandFitsBuilder":
        self._denoiser = denoiser
        return self

    def build(self) -> np.ndarray:
        if self._should_preprocess:
            self._fits_data = self._preprocess(self._fits_data)

        if self._mask_generator:
            mask = self._mask_generator.generate((self._fits_data.shape[0], self._fits_data.shape[1]))
            self._fits_data = self._mask_generator.apply_mask(self._fits_data)

        if self._denoiser:
            # TODO: fix this
            self._fits_data = self._denoiser.denoise(self._fits_data, mask)

        return self._fits_data

    @staticmethod
    def _preprocess(fits_data) -> np.ndarray:
        # Shift FITS data to be non-negative
        shift: float = np.min(fits_data)
        if shift < 0:
            fits_data += abs(shift)

        # TODO: Add more preprocessing steps
        return fits_data


class GalaxyFitsData:
    """ Data holder for a single galaxy's FITS file """

    def __init__(self, source_id: str, fits_data_list: np.ndarray):
        self.source_id: str = source_id

        self._band_data_map: Dict[str, Optional[BandFitsBuilder]] = {}
        for i, band in enumerate(FITS_BANDS):
            fits_data = fits_data_list[i]
            is_valid = self._validate_fits(fits_data)
            self._band_data_map[band] = BandFitsBuilder(fits_data) if is_valid else None

    @staticmethod
    def _validate_fits(fits_data: np.ndarray) -> bool:
        """ Determines whether the FITS data is a valid galaxy image """
        # Check if FITS data is empty or contains NaNs
        if not fits_data.any() or np.any(np.isnan(fits_data)):
            return False
        # TODO: Add more validation checks
        return True

    def get_band_data(self, band: str) -> Optional[BandFitsBuilder]:
        if band not in FITS_BANDS:
            raise ValueError(f"Invalid band: {band}, must be one of {FITS_BANDS}")
        return self._band_data_map[band]


class BatchFitsReader:
    """ Interface for reading batch files as a collection of FITS data """

    def __init__(self, batch_file_path: str):
        self.batch_file_path: str = batch_file_path
        self.batch_file: BatchFile = BatchFile.from_file(batch_file_path)

        # Create an index of FITS files for quick access
        self.fits_index: Dict[str, FileMetadata] = {}
        for fits_metadata in self.batch_file.metadata_list:
            self.fits_index[fits_metadata.get_file_name()] = fits_metadata

    @staticmethod
    def from_file(file_path: str) -> "BatchFitsReader":
        return BatchFitsReader(file_path)

    def get_fits(self, file_name: str, suffix: str = ".fits") -> GalaxyFitsData:
        """ Loads FITS data for a single galaxy """
        try:
            fits_file_like: BytesIO = self.fits_index[f"{file_name}{suffix}"].get_as_file_like()
            with fits.open(fits_file_like, memmap=False) as hdu_list:
                fits_data_list: np.ndarray = hdu_list[0].data

            return GalaxyFitsData(file_name, fits_data_list)
        except Exception as e:
            print(f"Error loading FITS data for {file_name}", file=sys.stderr)
            raise e

    def loop_fits(self) -> Generator[GalaxyFitsData, None, None]:
        for file_name in self.fits_index.keys():
            file_name: str
            yield self.get_fits(file_name, suffix="")

    def size(self) -> int:
        return len(self.batch_file.metadata_list)

    def get_files(self) -> List[FileMetadata]:
        return self.batch_file.metadata_list


# Class batch fits writer or something like that
# def save_fits(self, source_id: str, bin_id: str, raw_bytes: bytes):
#     fits_path = self._build_fits_path(source_id, bin_id)
#     os.makedirs(os.path.dirname(fits_path), exist_ok=True)
#
#     with open(fits_path, "wb") as file:
#         file.write(raw_bytes)

# class LinuxFitsInterface:
#     def load_fits(self, source_id: str, bin_id: str) -> GalaxyFitsData:
#         fits_path = self._build_fits_path(source_id, bin_id)
#         with fits.open(fits_path) as hdu_list:
#             fits_data_list: np.ndarray = hdu_list[0].data
#             hdu_list.close()
#
#         return GalaxyFitsData(source_id, fits_data_list)
#
#     def save_fits(self, source_id: str, bin_id: str, raw_bytes: bytes):
#         fits_path = self._build_fits_path(source_id, bin_id)
#         os.makedirs(os.path.dirname(fits_path), exist_ok=True)
#
#         with open(fits_path, "wb") as file:
#             file.write(raw_bytes)

class AbstractBatchFilePathGenerator:
    def generate(self, bin_id: str, batch_id: str) -> str:
        raise NotImplementedError


class ClothoDockerBatchFilePathGenerator(AbstractBatchFilePathGenerator):
    def generate(self, bin_id: str, batch_id: str) -> str:
        return f"/batch-fits-data/{bin_id}/{batch_id}.batch"


class ClothoTestingBatchFilePathGenerator(AbstractBatchFilePathGenerator):
    def generate(self, bin_id: str, batch_id: str) -> str:
        return f"/home/neo/sharedata/data/batch/{bin_id}/{batch_id}.batch"


class LocalTestingBatchFilePathGenerator(AbstractBatchFilePathGenerator):
    def generate(self, bin_id: str, batch_id: str) -> str:
        return f"C:/One/UCI/Alberto/data/batch/{bin_id}/{batch_id}.batch"


if __name__ == "__main__":
    import matplotlib.pyplot as plt
    from commons.models.mask_generators import CircleMaskGenerator

    reader: BatchFitsReader = BatchFitsReader.from_file("C:/One/UCI/Alberto/data/batch/26d9f5ea118f42abb1f7ad1862f931d0/00a494d9c7b44b3b945020c64576eb7e.batch")
    print(f"Loaded {reader.size()} FITS files:")
    print(reader.get_files())

    for i, galaxy_fits_data in enumerate(reader.loop_fits()):
        i: int
        galaxy_fits_data: GalaxyFitsData
        print(f"Loaded FITS data for galaxy {galaxy_fits_data.source_id}")

        if i == 0:
            target_band: str = "g"
            band_data: BandFitsBuilder = galaxy_fits_data.get_band_data(target_band)

            mask_generator: AbstractMaskGenerator = CircleMaskGenerator()
            plt.imshow(band_data.mask(mask_generator).build(), cmap="gray")
            plt.show()
