from enum import Enum

STARTING_PORT_NUMBER = 6500


class ContainerType(Enum):
    AUGMENT = "augment"
    BACKEND = "backend"
    FETCH = "fetch"
    FRONTEND = "frontend"
    RADON = "radon"

    @staticmethod
    def is_valid_container_type(container_type: str) -> bool:
        return any(container_type == item.value for item in ContainerType)

    def is_pipeline(self) -> bool:
        return self in {ContainerType.AUGMENT, ContainerType.FETCH, ContainerType.RADON}

    @staticmethod
    def is_valid_pipeline_type(container_type: str) -> bool:
        return ContainerType.is_valid_container_type(container_type) and ContainerType(container_type).is_pipeline()

    def get_image_tag(self) -> str:
        if self.is_pipeline():
            return f"pipeline-{self.value}"
        return self.value
