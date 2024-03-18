import random
from typing import List

import numpy as np
import scipy.ndimage as ndimage
from skimage.filters import gaussian


class AugmentResult:
    """ Data holder for the augmented image and the angle delta """

    def __init__(self, result_image: np.ndarray, angle_delta: int, metadata: str):
        self.result_image: np.ndarray = result_image
        self.angle_delta: int = angle_delta
        self.metadata: str = metadata

    def merge(self, other: "AugmentResult") -> "AugmentResult":
        """ Merge two results, returns a new result that uses the latter's result image """
        new_delta_angle: int = (self.angle_delta + other.angle_delta) % 180
        return AugmentResult(other.result_image, new_delta_angle, f"{self.metadata}_{other.metadata}")


class AbstractAugmenter:
    def random_augment(self, image: np.ndarray, count: int = 1) -> List[AugmentResult]:
        """
        Augment the image `count` times

        Args:
            image: the original galaxy image
            count: the number of augmented images to generate

        Returns:
            A list of augmented results
        """
        raise NotImplementedError


class RotationAugmenter(AbstractAugmenter):
    def random_augment(self, image: np.ndarray, count: int = 1) -> List[AugmentResult]:
        random_angles: np.ndarray = np.random.randint(0, 180, count)
        return [self._augment_rotate(image, angle) for angle in random_angles]

    @staticmethod
    def _augment_rotate(fits: np.ndarray, angle: int) -> AugmentResult:
        # Rotate FITS (clockwise, therefore subtract from 360 degrees)
        rotated: np.ndarray = ndimage.rotate(fits, 360 - angle, reshape=False)
        # Check & remove negative values
        shift: float = np.min(rotated)
        if shift < 0:
            rotated += abs(shift)
        return AugmentResult(rotated, angle, f"R{angle}")


class ResampleAugmenter(AbstractAugmenter):
    def random_augment(self, image: np.ndarray, count: int = 1) -> List[AugmentResult]:
        return [self._augment_resample(image, self._random_sample_count()) for _ in range(count)]

    @staticmethod
    def _random_sample_count() -> int:
        return int(np.random.uniform(15, 100))

    @staticmethod
    def _augment_resample(fits: np.ndarray, sample_count: int, brightness_modifier: int = 30) -> AugmentResult:
        assert np.all(fits >= 0), "Fits data must be non-negative"

        # Scale brightness
        fits: np.ndarray = brightness_modifier * fits

        # Repeat the input image to match the number of samples
        repeated_image: np.ndarray = np.repeat(fits[np.newaxis, ...], sample_count, axis=0)
        samples: np.ndarray = np.random.poisson(repeated_image)

        final_image: np.ndarray = np.sum(samples, axis=0)
        return AugmentResult(final_image, 0, f"S{sample_count}")


class BlurAugmenter(AbstractAugmenter):
    def random_augment(self, image: np.ndarray, count: int = 1) -> List[AugmentResult]:
        return [self._augment_blur(image, self._random_intensity()) for _ in range(count)]

    @staticmethod
    def _random_intensity() -> float:
        return np.random.uniform(0, 2)

    @staticmethod
    def _augment_blur(fits: np.ndarray, intensity: float) -> AugmentResult:
        return AugmentResult(gaussian(fits, intensity), 0, f"B{intensity:.2f}")


class RandomAugmenter(AbstractAugmenter):
    """ Randomly selects a set of augmenters and applies them to the image """

    def __init__(self, augmenters: List[AbstractAugmenter] = None):
        self.augmenters: List[AbstractAugmenter] = augmenters

        # If no augmenters are provided, use all of them
        if self.augmenters is None:
            self.augmenters = [RotationAugmenter(), ResampleAugmenter(), BlurAugmenter()]

    def random_augment(self, image: np.ndarray, count: int = 1) -> List[AugmentResult]:
        results: List[AugmentResult] = []

        for _ in range(count):
            augment_count: int = random.randint(1, len(self.augmenters))
            selected_augmenters: List[AbstractAugmenter] = random.sample(self.augmenters, augment_count)
            augment_result = None
            for augmenter in selected_augmenters:
                if augment_result is None:
                    augment_result = augmenter.random_augment(image)[0]
                else:
                    augment_result = augment_result.merge(augmenter.random_augment(augment_result.result_image)[0])
            results.append(augment_result)

        return results


if __name__ == "__main__":
    import matplotlib.pyplot as plt
    from commons.models.data_suppliers import GalaxyDataSupplier, DataSupplyResult
    from commons.utils.sql_utils import LocalPostgresClientFactory

    # Supply data
    postgres_client_factory = LocalPostgresClientFactory()
    data_supply: List[DataSupplyResult] = GalaxyDataSupplier(postgres_client_factory).supply(1)
    galaxy_fits = data_supply[0].data

    # Augment data
    augmenter = RandomAugmenter()
    augmented_images = augmenter.random_augment(galaxy_fits, 5)

    # Plot the original and augmented images
    fig, axes = plt.subplots(1, 6, figsize=(20, 5))
    axes[0].imshow(galaxy_fits, cmap="gray")
    axes[0].set_title("Original")
    for i, augmented_image in enumerate(augmented_images):
        axes[i + 1].imshow(augmented_image.result_image, cmap="gray")
        axes[i + 1].set_title(augmented_image.metadata)

    plt.tight_layout()
    plt.show()
