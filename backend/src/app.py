from typing import Any

from flask import Flask, jsonify
from flask_restful import Api

import commons.utils.log_util as log
from src.endpoints import PipelineEndpoint, PipelineStatusEndpoint


def create_app(options=None) -> Flask:
    flask_app: Flask = Flask(__name__)
    if options:
        flask_app.config.from_mapping({"TESTING": True})

    flask_api: Api = Api(flask_app)

    # Register endpoints
    PipelineEndpoint.bind_self(flask_api)
    PipelineStatusEndpoint.bind_self(flask_api)

    @flask_app.errorhandler(404)
    def handle_not_found(e: Any):
        return jsonify(error=str(e)), 404

    return flask_app


# Variable accessed by the entrypoint
app: Flask = create_app()

if __name__ == "__main__":
    # Warning: this starts the *debug* server (not secure)
    # - Use "gunicorn --bind 0.0.0.0:5000 app:app" for production
    #   - 1st "app" refers to this file, aka app.py
    #   - 2nd "app" refers to the global variable 'app'
    # For production, use Docker file to start the app
    log.info("Starting debug radon backend server...")
    app.run(host="0.0.0.0", port=5000)
