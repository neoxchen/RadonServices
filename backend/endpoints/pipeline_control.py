import traceback

import requests
from docker.types import Mount
from flask import make_response, request
from flask_restful import Resource

import utils.log_util as log
from endpoints.wrappers import safe_request
from shared_states import PIPELINE_CONTAINERS, get_container_by_id, get_container_address
from utils.docker_interface import boot_container_until_success


class PipelineEndpoint(Resource):
    path = "/pipelines/<pipeline_type>"

    @staticmethod
    def bind_self(api):
        api.add_resource(PipelineEndpoint, PipelineEndpoint.path)

    @safe_request
    def get(self, pipeline_type, uid):
        """
        Gets the status of the active pipelines, allowing for an optional pipeline type
        - if no type is specified, it will retrieve the status of all pipelines
        """
        if pipeline_type == "all":
            pipeline_json = {}
            for pipeline_type, containers in PIPELINE_CONTAINERS.items():
                pipeline_json[pipeline_type] = []
                for container in containers:
                    hostname = container.container.attrs["Config"]["Hostname"]
                    pipeline_json[pipeline_type].append({
                        "id": container.container_id,
                        "name": container.container.name,
                        "hostname": hostname,
                        "port": container.port,
                        "status": container.container.attrs["State"]["Status"]
                    })
            return make_response({"message": "OK", "pipelines": pipeline_json}, 200)

        # Check if pipeline type is valid
        valid_pipeline_types = ["fetch", "radon"]
        pipeline_type = pipeline_type.lower()
        if pipeline_type not in valid_pipeline_types:
            return make_response({
                "error": f"Invalid pipeline type {pipeline_type}!",
                "valid_types": valid_pipeline_types
            }, 400)

        # Get the list of containers of the corresponding pipeline type
        containers = []
        for container in PIPELINE_CONTAINERS[f"pipeline-{pipeline_type}"]:
            hostname = container.container.attrs["Config"]["Hostname"]
            containers.append({
                "id": container.container_id,
                "name": container.container.name,
                "hostname": hostname,
                "port": container.port,
                "status": container.container.attrs["State"]["Status"]
            })

        return make_response({"message": "OK", "containers": containers}, 200)

    @safe_request
    def post(self, pipeline_type, uid):
        """
        Creates a new pipeline of the corresponding pipeline type
        """
        # Check if pipeline type is valid
        valid_pipeline_types = ["fetch", "radon"]
        pipeline_type = pipeline_type.lower()
        if pipeline_type not in valid_pipeline_types:
            return make_response({
                "error": f"Invalid pipeline type {pipeline_type}!",
                "valid_types": valid_pipeline_types
            }, 400)

        # Dynamically spin up the corresponding pipeline
        log.request(uid, f"Spinning up a new '{pipeline_type}' pipeline...")

        fits_volume = Mount(target="/fits-data", source="/home/neo/data/fits", type="bind")
        new_ports = boot_container_until_success(
            f"pipeline-{pipeline_type}",
            environment={
                # Note: port & container ID information will be automatically added
            },
            mounts=[fits_volume],
            network="radonservices_radon_network"  # note the radonservices prefix because compose adds that
        )

        return make_response({
            "message": "OK",
            "new_pipeline": pipeline_type,
            "ports": new_ports
        }, 200)

    @safe_request
    def delete(self, pipeline_type, uid):
        """ Deletes the pipeline of the corresponding type """
        # Check if pipeline type is valid
        valid_pipeline_types = ["fetch", "radon"]
        pipeline_type = pipeline_type.lower()
        if pipeline_type not in valid_pipeline_types:
            return make_response({
                "error": f"Invalid pipeline type {pipeline_type}!",
                "valid_types": valid_pipeline_types
            }, 400)

        # Dynamically spin up the corresponding pipeline
        container_id = request.get_json()["container_id"]
        log.request(uid, f"Terminating the '{pipeline_type}' pipeline with container ID {container_id}...")

        # Build URL to send to the pipeline
        container = get_container_by_id(container_id)
        if container is None:
            return make_response({"error": f"Container with ID {container_id} does not exist!"}, 404)

        container_url = f"{get_container_address(container_id)}/control"
        try:
            log.request(uid, f"Sending stop request to '{container_url}'...")
            requests.post(container_url, json={"action": "stop"})
        except requests.exceptions.ConnectionError:
            return make_response({
                "error": f"Container with ID {container_id} is not running!",
                "details": traceback.format_exc()
            }, 404)

        return make_response({"message": "OK"}, 200)
