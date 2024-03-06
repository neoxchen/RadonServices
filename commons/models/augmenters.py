from typing import List, Tuple

import numpy as np
import scipy.ndimage as ndimage


class AugmentResult:
    """ Data holder for the augmented image and the angle delta """

    def __init__(self, result_image: np.ndarray, angle_delta: int):
        self.result_image: np.ndarray = result_image
        self.angle_delta: int = angle_delta


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
        random_angles = np.random.randint(0, 180, count)
        return [AugmentResult(*self._augment_rotate(image, angle)) for angle in random_angles]

    @staticmethod
    def _augment_rotate(fits: np.ndarray, angle: int) -> Tuple[np.ndarray, int]:
        # Rotate FITS (clockwise, therefore subtract from 360 degrees)
        rotated = ndimage.rotate(fits, 360 - angle, reshape=False)
        # Check & remove negative values
        shift = np.min(rotated)
        if shift < 0:
            rotated += abs(shift)
        return rotated, angle


if __name__ == "__main__":
    import matplotlib.pyplot as plt
    from commons.models.data_suppliers import GalaxyDataSupplier
    from commons.models.mask_generators import CircleMaskGenerator
    from commons.utils.sql_utils import LocalPostgresClientFactory

    # Create local database client
    postgres_client_factory = LocalPostgresClientFactory()

    # Preview denoising results
    temp_count = 2
    temp_data_supply = GalaxyDataSupplier(postgres_client_factory).supply(temp_count)
    temp_mask = CircleMaskGenerator().generate(temp_data_supply[0].data.shape)
    temp_denoiser = LowPassDenosier()
    temp_denoised_data = [temp_denoiser.denoise(data_supply.data, temp_mask) for data_supply in temp_data_supply]

    fig, ax = plt.subplots(temp_count, 3, figsize=(5, 4))
    for i in range(temp_count):
        ax[i, 0].imshow(temp_data_supply[i].data, cmap="gray")
        ax[i, 0].set_title(f"Original")
        ax[i, 0].axis('off')
        ax[i, 1].imshow(temp_denoised_data[i], cmap="gray")
        ax[i, 1].set_title(f"Denoised")
        ax[i, 1].axis('off')
        ax[i, 2].imshow(temp_data_supply[i].data - temp_denoised_data[i], cmap="gray")
        ax[i, 2].set_title(f"Diff")
        ax[i, 2].axis('off')

    plt.tight_layout()
    plt.show()