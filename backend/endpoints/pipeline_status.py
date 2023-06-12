from flask import make_response, request
from flask_restful import Resource

from endpoints.wrappers import safe_request
from shared_states import PIPELINE_ITERATION_STATUS


class PipelineStatusEndpoint(Resource):
    path = "/pipelines/status/<pipeline_type>"

    @staticmethod
    def bind_self(api):
        api.add_resource(PipelineStatusEndpoint, PipelineStatusEndpoint.path)

    @safe_request
    def get(self, pipeline_type, uid):
        """
        Gets the status of the active pipelines, allowing for an optional pipeline type
        - if no type is specified, it will retrieve the status of all pipelines
        """
        # Check if pipeline type is valid
        valid_pipeline_types = ["fetch", "radon"]
        pipeline_type = pipeline_type.lower()
        if pipeline_type not in valid_pipeline_types:
            return make_response({
                "error": f"Invalid pipeline type {pipeline_type}!",
                "valid_types": valid_pipeline_types
            }, 400)

        return make_response({"message": "OK", "status": PIPELINE_ITERATION_STATUS[pipeline_type]}, 200)

    @safe_request
    def post(self, pipeline_type, uid):
        """ Updates the status of the current pipeline """
        # Example request body:
        # {
        #     "container_id": "123",
        #     "iteration": 123,
        #     "galaxies": ["123"],
        #     "successes": ["123"],
        #     "fails": ["123"]
        # }

        body = request.get_json()
        PIPELINE_ITERATION_STATUS[pipeline_type] = {
            "container_id": body["container_id"],
            "iteration": body["iteration"],
            "galaxies": body["galaxies"],
            "successes": body["successes"],
            "fails": body["fails"]
        }

        return make_response({"message": "OK"}, 200)
