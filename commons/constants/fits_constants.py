import os

FITS_BANDS = ["g", "r", "i", "z"]
FITS_DIRECTORY_PATH: str = os.getenv("FITS_DIRECTORY_PATH", default="/fits-data")
