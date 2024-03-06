import sys
from typing import Optional

from commons.models.fits_interfaces import AbstractFitsInterface, GalaxyFitsData


class Galaxy:
    def __init__(self, source_id: str, bin_id: str, ra: float, dec: float, gal_prob: float, status: str, failed_attempts: int):
        self.source_id: str = source_id
        self.bin_id: str = bin_id
        self.ra: float = ra
        self.dec: float = dec
        self.gal_prob: float = gal_prob
        self.status: str = status
        self.failed_attempts: int = failed_attempts

        self._fits_data: Optional[GalaxyFitsData] = None

    def get_fits_data(self, fits_interface: AbstractFitsInterface) -> Optional[GalaxyFitsData]:
        if not self._fits_data:
            self._load_fits_data(fits_interface)
        return self._fits_data

    def _load_fits_data(self, fits_interface: AbstractFitsInterface) -> None:
        try:
            self._fits_data = fits_interface.load_fits(self.source_id, self.bin_id)
        except Exception as e:
            print(f"Error loading FITS data for source_id={self.source_id}, bin_id={self.bin_id}", file=sys.stderr)
            print(e, file=sys.stderr)


if __name__ == "__main__":
    import matplotlib.pyplot as plt
    from commons.models.fits_interfaces import BandFitsBuilder, LocalTestingFitsInterface

    galaxy: Galaxy = Galaxy("2305889330436758656", "6", 0.0, 0.0, 0.0, "Fetched", 0)
    fits_interface: AbstractFitsInterface = LocalTestingFitsInterface()
    galaxy_fits_data: GalaxyFitsData = galaxy.get_fits_data(fits_interface)

    target_band: str = "g"
    band_data_builder: BandFitsBuilder = galaxy_fits_data.get_band_data(target_band)

    plt.imshow(band_data_builder.build(), cmap="gray")
    plt.show()
