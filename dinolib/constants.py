from enum import Enum


class ContainerType(Enum):
    AUGMENT = "augment"
    BACKEND = "backend"
    FETCH = "fetch"
    FRONTEND = "frontend"
    RADON = "radon"

    def is_pipeline(self) -> bool:
        return self in {ContainerType.AUGMENT, ContainerType.FETCH, ContainerType.RADON}

    def get_image_tag(self) -> str:
        if self.is_pipeline():
            return f"pipeline-{self.value}"
        # TODO: optimize this
        elif self == ContainerType.BACKEND:
            return "orchestrator"
        elif self == ContainerType.FRONTEND:
            return "web-interface"
