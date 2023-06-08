from flask import make_response
from flask_restful import Resource

from endpoints.wrappers import safe_request


class PipelineStatusEndpoint(Resource):
    path = "/pipelines/status/<pipeline_id>"

    @staticmethod
    def bind_self(api):
        api.add_resource(PipelineStatusEndpoint, PipelineStatusEndpoint.path)

    @safe_request
    def get(self, pipeline_id, uid):
        """
        Gets the status of the active pipelines, allowing for an optional pipeline type
        - if no type is specified, it will retrieve the status of all pipelines
        """
        return make_response({"message": "OK", "status": pipeline_id}, 200)

    @safe_request
    def post(self, pipeline_id, uid):
        """
        Creates a new pipeline of the corresponding pipeline type
        """
        return make_response({
            "message": "OK",
            "new_status": pipeline_id
        }, 200)
