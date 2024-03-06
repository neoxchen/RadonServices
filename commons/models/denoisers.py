import numpy as np


class AbstractDenoiser:
    def denoise(self, image: np.ndarray, mask: np.ndarray) -> np.ndarray:
        raise NotImplementedError


class LowPassDenosier(AbstractDenoiser):
    def denoise(self, image: np.ndarray, mask: np.ndarray) -> np.ndarray:
        """ Most primitive denoising method by subtracting the mean brightness of non-masked pixels """
        # Calculate the average of the non-masked pixels as ambient noise
        ambient_noise = np.mean(image * ~mask)
        denoised = image - ambient_noise
        denoised[denoised < 0] = 0
        assert np.min(denoised) >= 0
        return denoised


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
