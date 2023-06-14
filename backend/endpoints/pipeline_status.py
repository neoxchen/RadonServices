from flask import make_response, request
from flask_restful import Resource

from endpoints.wrappers import safe_request
from shared_states import CONTAINER_STATUS


class PipelineStatusEndpoint(Resource):
    path = "/pipelines/status/<container_id>"

    @staticmethod
    def bind_self(api):
        api.add_resource(PipelineStatusEndpoint, PipelineStatusEndpoint.path)

    @safe_request
    def get(self, container_id, uid):
        """
        Gets the status of the active pipelines, allowing for an optional pipeline type
        - if no type is specified, it will retrieve the status of all pipelines
        """
        return make_response({"message": "OK", "status": CONTAINER_STATUS[container_id]}, 200)

    @safe_request
    def post(self, container_id, uid):
        """ Updates the status of the current pipeline """
        # Example request body:
        # {
        #     "iteration": 123,
        #     "galaxies": ["123"],
        #     "successes": ["123"],
        #     "fails": ["123"]
        # }

        body = request.get_json()
        CONTAINER_STATUS[container_id] = {
            "iteration": body["iteration"],
            "galaxies": body["galaxies"],
            "successes": body["successes"],
            "fails": body["fails"]
        }

        return make_response({"message": "OK"}, 200)
