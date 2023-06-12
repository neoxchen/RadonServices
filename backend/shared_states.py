from collections import defaultdict
from typing import Dict, List, Optional

from docker.models.containers import Container


###########################
# Docker Container States #
###########################
class DynamicContainer:
    def __init__(self, container: Container, container_id: str,
                 pipeline_type: str, port: int):
        self.container = container
        self.container_id = container_id

        self.pipeline_type = pipeline_type
        self.port = port

    def __str__(self):
        return f"DynamicContainer({self.container_id}, {self.pipeline_type}, {self.port})"


# Map of { pipeline type => list of DynamicContainer objects }
PIPELINE_CONTAINERS: Dict[str, List[DynamicContainer]] = defaultdict(list)


def get_pipeline_type(pipeline_id: str) -> Optional[str]:
    for pipeline_type, containers in PIPELINE_CONTAINERS.items():
        for container in containers:
            if container.container_id == pipeline_id:
                return pipeline_type
    return None


##########################
# Pipeline Status States #
##########################
# Map of { pipeline type => JSON object (dict) }
# - contains only the data of the latest iteration (previous iterations are overridden)
PIPELINE_ITERATION_STATUS = defaultdict(dict)