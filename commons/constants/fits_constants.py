import os

BATCH_FITS_SIZE = 512
BATCH_FITS_SUFFIX = ".batch"

FITS_BANDS = ["g", "r", "i", "z"]
FITS_DIRECTORY_PATH: str = os.getenv("FITS_DIRECTORY_PATH", default="/fits-data")
