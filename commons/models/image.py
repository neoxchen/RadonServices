from typing import Any, Dict

import numpy as np
import scipy.ndimage as ndimage


class AbstractImage:
    def __init__(self, metadata: Dict[str, Any]):
        self.metadata: Dict[str, Any] = metadata

    def get_image(self) -> np.ndarray:
        raise NotImplementedError

    def rotate(self, degrees: float) -> None:
        raise NotImplementedError


class SingleChannelImage(AbstractImage):
    def __init__(self, image: np.ndarray, metadata: dict):
        super().__init__(metadata)
        self.image: np.ndarray = image

    def get_image(self) -> np.ndarray:
        return self.image

    def rotate(self, degrees: float) -> None:
        """ Rotate the image clockwise by `degrees` """
        rotated: np.ndarray = ndimage.rotate(self.image, 360 - degrees, reshape=False)
        # Check & remove negative values
        shift: float = np.min(rotated)
        if shift < 0:
            rotated += abs(shift)
        self.image = rotated


if __name__ == "__main__":
    import matplotlib.pyplot as plt

    matrix: np.ndarray = np.zeros((40, 40))
    matrix[7:10, :] = 1
    matrix[:, 7:10] = 1

    image: AbstractImage = SingleChannelImage(matrix, {})
    original_image: np.ndarray = np.copy(image.get_image())

    image.rotate(30)
    rotated_image: np.ndarray = np.copy(image.get_image())

    # Plot the original and rotated images side by side
    fig, ax = plt.subplots(1, 2, figsize=(10, 5))

    ax[0].imshow(original_image, cmap="gray")
    ax[0].set_title("Original Image")

    ax[1].imshow(rotated_image, cmap="gray")
    ax[1].set_title("Rotated Image")

    plt.tight_layout()
    plt.show()
