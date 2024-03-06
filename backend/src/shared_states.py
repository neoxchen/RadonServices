from collections import defaultdict
from typing import Dict, List, Optional

from docker.models.containers import Container


###########################
# Docker Container States #
###########################
class DynamicContainer:
    def __init__(self, container: Container, container_id: str, pipeline_type: str, port: int):
        self.container: Container = container
        self.container_id: str = container_id
        self.pipeline_type: str = pipeline_type
        self.port: int = port

    def __str__(self) -> str:
        return f"DynamicContainer({self.container_id}, {self.pipeline_type}, {self.port})"


# Map of { pipeline type => list of DynamicContainer objects }
PIPELINE_CONTAINERS: Dict[str, List[DynamicContainer]] = defaultdict(list)


def get_pipeline_type(pipeline_id: str) -> Optional[str]:
    for pipeline_type, containers in PIPELINE_CONTAINERS.items():
        for container in containers:
            if container.container_id == pipeline_id:
                return pipeline_type
    return None


def get_container_by_id(container_id: str) -> Optional[DynamicContainer]:
    for containers in PIPELINE_CONTAINERS.values():
        for container in containers:
            if container.container_id == container_id:
                return container
    return None


def get_container_address(container_id: str):
    """ Returns the address of the container with the given ID """
    container: Optional[DynamicContainer] = get_container_by_id(container_id)
    if container is None:
        return None

    ip: str = container.container.attrs['Config']['Hostname']
    return f"http://{ip}:{container.port}"


##########################
# Pipeline Status States #
##########################
# Map of { container_id => JSON object (dict) }
# - contains only the data of the latest iteration (previous iterations are overridden)
CONTAINER_STATUS: Dict[str, Dict[str, any]] = defaultdict(dict)
