import numpy as np
import scipy.ndimage

from commons.models.mask_generators import AbstractMaskGenerator


class RadonTransformResult:
    """ Data holder for the result of the radon transform """

    def __init__(self, raw_image: np.ndarray, sinogram: np.ndarray, mask_generator: AbstractMaskGenerator):
        self.raw_image = raw_image
        self.sinogram = sinogram
        self.mask_generator = mask_generator

    def get_rotation(self) -> float:
        offset, rotation = np.unravel_index(self.sinogram.argmax(), self.sinogram.shape)
        return rotation

    def get_orthogonal(self) -> float:
        rotation = self.get_rotation()
        return (rotation + 90) % 180


class RadonTransformer:
    def __init__(self, mask_generator: AbstractMaskGenerator):
        self.mask_generator = mask_generator

    def transform(self, raw_image: np.ndarray, fineness: int = 181) -> RadonTransformResult:
        """
        Calculates the radon transform of the FITS image
        - assuming the galaxy is centered

        Args:
            raw_image (np.ndarray): the FITS image
            fineness (int): the number of slices between 0 and 180 degrees

        Returns:
            (np.ndarray): the resulting sinogram representation
        """
        raw_image_shape = raw_image.shape
        if raw_image_shape[0] != raw_image_shape[1]:
            raise ValueError(f"The image must be a square, got {raw_image_shape} instead")

        # Mask the image
        raw_image = self.mask_generator.apply_mask(raw_image)

        sinogram = np.zeros((40, fineness))
        thetas = np.linspace(0, np.pi, fineness)
        for i, (sin_theta, cos_theta) in enumerate(zip(np.sin(thetas), np.cos(thetas))):
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
            interpolated_image = scipy.ndimage.map_coordinates(raw_image, transformed_coords_3d.transpose((2, 0, 1)))
            sums = np.sum(interpolated_image, axis=0)
            sinogram[:, i] = sums
        return RadonTransformResult(raw_image, sinogram, self.mask_generator)


if __name__ == "__main__":
    import matplotlib.pyplot as plt
    from commons.models.data_suppliers import EllipseDataSupplier
    from commons.models.mask_generators import CircleMaskGenerator

    # Preview Radon Transform
    temp_count = 2
    temp_data_supply = EllipseDataSupplier((8, 18), (5, 10)).supply(temp_count)
    temp_transformer = RadonTransformer(CircleMaskGenerator())
    temp_transform_results = [temp_transformer.transform(data_supply.data) for data_supply in temp_data_supply]

    fig, ax = plt.subplots(2, temp_count, figsize=(9, 4))
    for i in range(temp_count):
        ax[0, i].imshow(temp_data_supply[i].data, cmap="gray")
        ax[0, i].set_title(f"Original")
        ax[0, i].axis('off')
        ax[1, i].imshow(temp_transform_results[i].sinogram, cmap="viridis", aspect="auto")
        ax[1, i].set_title(f"Sinogram - R{temp_transform_results[i].get_rotation()}")
        ax[1, i].axis('off')

    plt.tight_layout()
    plt.show()
