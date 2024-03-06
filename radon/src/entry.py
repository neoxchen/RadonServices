import os
import time
from threading import Thread
from typing import Optional

from flask import Flask, make_response, request
from werkzeug.serving import make_server

from script import run_script, set_stop_script

# Fetch port to use (must be present)
PORT = int(os.getenv("PORT"))
print(f"Configured environment variable PORT as {PORT}")


class ServerThread(Thread):
    def __init__(self, app):
        Thread.__init__(self, daemon=False)
        self.server = make_server("0.0.0.0", PORT, app)
        self.ctx = app.app_context()
        self.ctx.push()

    def run(self):
        print("Starting API server...")
        self.server.serve_forever()

    def shutdown(self):
        print("Stopping API server...")
        self.server.shutdown()


server: Optional[ServerThread] = None


def start_server():
    global server
    app = Flask(__name__)

    @app.route("/control", methods=["POST"])
    def control_pipeline():
        body = request.get_json()
        action = body.get("action")
        if action != "stop":
            return make_response({"error": "Invalid action"}, 400)

        set_stop_script()
        Thread(target=stop_server).start()
        return make_response({"message": "Successfully stopped the pipeline"}, 200)

    server = ServerThread(app)
    server.start()


def stop_server():
    global server
    if server is None:
        return
    # Sleep so that the response can be sent
    time.sleep(1)
    # Stop the API server
    server.shutdown()


if __name__ == "__main__":
    print("Starting fetch pipeline...")
    # Start fetch script automatically
    script_thread = Thread(target=run_script, daemon=False)
    script_thread.start()

    # Start flask server
    start_server()
