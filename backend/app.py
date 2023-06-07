from flask import Flask, jsonify
from flask_restful import Api

import utils.log_util as log
from endpoints import PipelineEndpoint, PipelineStatusEndpoint


def create_app(options=None):
    app = Flask(__name__)
    if options:
        app.config.from_mapping({"TESTING": True})

    api = Api(app)

    # Register endpoints
    PipelineEndpoint.bind_self(api)
    PipelineStatusEndpoint.bind_self(api)

    # Handle default error cases
    @app.errorhandler(404)
    def resource_not_found(e):
        return jsonify(error=str(e)), 404

    # TODO: Initialize load balancer
    # load_balancer.initialize()

    return app


app = create_app()

if __name__ == "__main__":
    # Warning: this starts the *debug* server (not secure)
    # - Use "gunicorn --bind 0.0.0.0:5000 app:app" for production
    #   - 1st "app" refers to this file, aka app.py
    #   - 2nd "app" refers to the global variable 'app'
    # For production, use Docker file to start the app
    log.info("Starting radon backend server...")
    app.run(host="0.0.0.0", port=5000)
