# The classes in this file represent an individual row in the corresponding database tables
# Schema version: 4

class SqlGalaxy:
    def __init__(self, uid: int, source_id: str, ra: float, dec: float, gal_prob: float, status: str, failed_attempts: int):
        self.uid: int = uid
        self.source_id: str = source_id
        self.ra: float = ra
        self.dec: float = dec
        self.gal_prob: float = gal_prob
        self.status: str = status
        self.failed_attempts: int = failed_attempts

    def __repr__(self):
        return f"Galaxy({self.uid}, {self.source_id}, {self.ra}, {self.dec}, {self.gal_prob}, {self.status}, {self.failed_attempts})"


class SqlBand:
    def __init__(self, band_uid: int, source_id: str, band: str, bin_id: str, batch_id: str, fits_index: int, has_error: bool):
        self.uid: int = band_uid
        self.source_id: str = source_id
        self.band: str = band
        self.bin_id: str = bin_id
        self.batch_id: str = batch_id
        self.fits_index: int = fits_index
        self.has_error: bool = has_error

    def __repr__(self):
        return f"Band({self.uid}, {self.source_id}, {self.band}, {self.bin_id}, {self.batch_id}, {self.fits_index}, {self.has_error})"


class SqlRotation:
    def __init__(self, band_uid: int, has_data: bool, degree: float, total_error: float, running_count: int):
        self.uid: int = band_uid
        self.has_data: bool = has_data
        self.degree: float = degree
        self.total_error: float = total_error
        self.running_count: int = running_count


class SqlEllipticity:
    def __init__(self, band_uid: int, has_data: bool):
        # TODO: Implement this class
        raise NotImplementedError("SqlEllipticity is not implemented")
