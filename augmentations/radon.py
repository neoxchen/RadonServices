import numpy as np
from scipy.ndimage import map_coordinates

# Radon variables
THETAS = np.linspace(0, np.pi, 181)
SIN_THETAS = np.sin(THETAS)
COS_THETAS = np.cos(THETAS)

# Define the circle mask
SHAPE = (40, 40)
CENTER = [dim // 2 for dim in SHAPE]
RADIUS = SHAPE[0] // 2
Y_COORDS, X_COORDS = np.ogrid[-CENTER[0]:SHAPE[0] - CENTER[0], -CENTER[1]:SHAPE[1] - CENTER[1]]
MASK = X_COORDS * X_COORDS + Y_COORDS * Y_COORDS <= RADIUS * RADIUS


def _radon_transform(image: np.ndarray) -> np.ndarray:
    """ Assuming image is already circle-masked """
    sinogram = np.zeros((40, len(THETAS)))
    for i, (sin_theta, cos_theta) in enumerate(zip(SIN_THETAS, COS_THETAS)):
        matrix = np.array([
            [cos_theta, sin_theta, -20 * (cos_theta + sin_theta - 1)],
            [-sin_theta, cos_theta, -20 * (cos_theta - sin_theta - 1)],
            [0, 0, 1]
        ])
        y_coords, x_coords = np.indices((40, 40))
        homogeneous_coords = np.stack((x_coords, y_coords, np.ones_like(x_coords)), axis=-1)
        homogeneous_coords_2d = homogeneous_coords.reshape((-1, 3))
        transformed_coords = np.dot(homogeneous_coords_2d, matrix.T)
        transformed_coords_2d = transformed_coords[:, :2] / transformed_coords[:, 2, np.newaxis]
        transformed_coords_3d = transformed_coords_2d.reshape((40, 40, 2))
        interpolated_image = map_coordinates(image, transformed_coords_3d.transpose((2, 0, 1)))
        sums = np.sum(interpolated_image, axis=0)
        sinogram[:, i] = sums
    return sinogram


def estimate_rotation(image: np.ndarray) -> int:
    """ Estimate the rotation of a galaxy """
    sinogram = _radon_transform(image)
    _offset, rotation = np.unravel_index(sinogram.argmax(), sinogram.shape)
    return rotation
