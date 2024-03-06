import traceback
import uuid
from functools import wraps
from time import time

import commons.utils.log_util as log
from flask import request


def safe_request(method):
    """ Decorator to make the HTTP request safe (logging + error catching) """

    @wraps(method)
    def wrapper(*args, **kwargs):
        request_uid = str(uuid.uuid4())[:8]

        log.request(request_uid, f"Incoming {request.method} request at endpoint \"{request.url}\"!")
        if request.data:
            log.request(request_uid, f"Request body: {request.data.decode()}!")

        try:
            start_time = time()
            response = method(*args, **kwargs, uid=request_uid)
            log.request(request_uid, f"Responding with code {response.status_code} in {int((time() - start_time) * 1000)} ms")
            if response.data:
                log.request(request_uid, f"Responding with payload: {response.data.decode()}")
        except Exception as e:
            log.error(f"[{request_uid}] An internal exception has been caught by safe request:")
            log.error(f"[{request_uid}] {e}")
            log.error(f"[{request_uid}] {traceback.format_exc()}")
            return {"message": "Internal server error, see console for details"}, 500

        return response

    return wrapper
