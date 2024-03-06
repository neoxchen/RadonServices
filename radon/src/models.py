from typing import List


class SineApproximation:
    def __init__(self, level: int, a: float, b: float, c: float, d: float, rmse: float):
        self.level = level
        self.a = a
        self.b = b
        self.c = c
        self.d = d
        self.rmse = rmse

    def __repr__(self):
        return f"SineApproximation(level={self.level}, a={self.a}, b={self.b}, c={self.c}, d={self.d}, rmse={self.rmse})"


class NormalDistribution:
    def __init__(self, window: int, mean: float, std_dev: float):
        self.window = window
        self.mean = mean
        self.std_dev = std_dev

    def __repr__(self):
        return f"NormalDistribution(window={self.window}, mean={self.mean}, std_dev={self.std_dev})"


class RadonBandResult:
    def __init__(self, band_name: str, degree: int, sine_approximations: List[SineApproximation], normal_distributions: List[NormalDistribution]):
        self.band_name = band_name
        self.degree = degree
        self.sine_approximations = sine_approximations
        self.normal_distributions = normal_distributions

    def __repr__(self):
        return f"RadonBandResult(band_name={self.band_name}, degree={self.degree}, sine_approximations={self.sine_approximations}, normal_distributions={self.normal_distributions})"


class RadonGalaxyResult:
    def __init__(self, source_id: str, bin_id: int, band_results: List[RadonBandResult], is_error=False):
        self.source_id = source_id
        self.bin_id = bin_id
        self.band_results = band_results
        self.is_error = is_error

    def __repr__(self):
        if self.is_error:
            return f"RadonGalaxyResult(source_id={self.source_id}, bin_id={self.bin_id}, is_error={self.is_error})"
        return f"RadonGalaxyResult(source_id={self.source_id}, bin_id={self.bin_id}, band_results={self.band_results})"
