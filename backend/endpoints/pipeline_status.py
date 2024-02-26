from flask import make_response, request
from flask_restful import Resource

from endpoints.wrappers import safe_request
from shared_states import CONTAINER_STATUS, get_pipeline_type, PIPELINE_CONTAINERS


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
        CONTAINER_STATUS[container_id] = request.get_json()
        return make_response({"message": "OK"}, 200)

    @safe_request
    def delete(self, container_id, uid):
        """ Deletes the status of the current pipeline """
        # Remove from pipeline dict
        pipeline_type = get_pipeline_type(container_id)
        if pipeline_type is not None:
            to_remove = None
            for container in PIPELINE_CONTAINERS[pipeline_type]:
                if container.container_id == container_id:
                    to_remove = container
                    break
            if to_remove is not None:
                PIPELINE_CONTAINERS[pipeline_type].remove(to_remove)

        # Remove from status dict
        del CONTAINER_STATUS[container_id]

        return make_response({"message": "OK"}, 200)
