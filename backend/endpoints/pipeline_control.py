from docker.types import Mount
from flask import make_response
from flask_restful import Resource

import utils.log_util as log
from endpoints.wrappers import safe_request
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
            return make_response({"message": "OK"}, 200)

        return make_response({"message": "OK", "pipelines": pipeline_type}, 200)

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
                "PIPELINE_TYPE": pipeline_type
            },
            mounts=[fits_volume],
            network="radonservices_radon_network"  # note the radonservices prefix because compose adds that
        )

        return make_response({
            "message": "OK",
            "new_pipeline": pipeline_type,
            "ports": new_ports
        }, 200)
