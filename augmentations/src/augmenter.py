import random
from typing import Tuple

import numpy as np
from scipy.ndimage import rotate
from skimage.filters import gaussian


def _augment_rotate(fits: np.ndarray, angle: int) -> Tuple[np.ndarray, int]:
    # Rotate FITS (clockwise, therefore subtract from 360 degrees)
    rotated = rotate(fits, 360 - angle, reshape=False)
    # Check & remove negative values
    shift = np.min(rotated)
    if shift < 0:
        rotated += abs(shift)
    return rotated, angle


def _augment_resample(fits: np.ndarray, sample_count: int, brightness_modifier=30) -> Tuple[np.ndarray, int]:
    assert np.all(fits >= 0), "Fits data must be non-negative"
    # fits = np.clip(fits, 0, 100)

    # Scale brightness
    fits = brightness_modifier * fits

    # Repeat the input image to match the number of samples
    repeated_image = np.repeat(fits[np.newaxis, ...], sample_count, axis=0)
    samples = np.random.poisson(repeated_image)
    final_image = np.sum(samples, axis=0)
    return final_image, 0


def _augment_blur(fits: np.ndarray, intensity: float) -> Tuple[np.ndarray, int]:
    return gaussian(fits, intensity), 0


augmentations = {
    _augment_rotate: lambda: int(np.random.uniform(0, 180)),
    _augment_resample: lambda: int(np.random.uniform(15, 100)),
    _augment_blur: lambda: np.random.uniform(0, 2)
}


def random_augment(fits: np.ndarray) -> Tuple[np.ndarray, int]:
    current_fits = fits
    delta_rotation = 0

    shuffled = list(augmentations.items())
    random.shuffle(shuffled)
    for augmentation, rng in shuffled:
        if np.random.uniform(0, 1) > 0.5:
            new_fits, new_delta_rotation = augmentation(current_fits, rng())
            current_fits = new_fits
            delta_rotation += new_delta_rotation

    return current_fits, delta_rotation
