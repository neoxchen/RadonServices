from typing import Tuple

import numpy as np


class AbstractMaskGenerator:
    def generate(self, shape: Tuple[int, int]) -> np.ndarray:
        raise NotImplementedError

    def apply_mask(self, image: np.ndarray) -> np.ndarray:
        raise NotImplementedError


class CircleMaskGenerator(AbstractMaskGenerator):
    def generate(self, shape: Tuple[int, int]) -> np.ndarray:
        # TODO: confirm typing
        center: Tuple[int, int] = (shape[0] // 2, shape[1] // 2)
        radius: int = shape[0] // 2
        ys, xs = np.ogrid[-center[0]:shape[0] - center[0], -center[1]:shape[1] - center[1]]
        mask: np.ndarray = xs ** 2 + ys ** 2 <= radius ** 2
        return mask

    def apply_mask(self, image: np.ndarray) -> np.ndarray:
        mask: np.ndarray = self.generate((image.shape[0], image.shape[1]))
        return image * mask
