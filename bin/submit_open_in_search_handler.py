# Copyright 2024 Splunk Inc.
# Needed to import libraries in /lib folder
import sys, os

from splunk.persistconn.application import PersistentServerConnectionApplication

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from base_rest import BaseRestUtils

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

class SubmitOpenInSearchHandler(PersistentServerConnectionApplication, BaseRestUtils):

    def __init__(self, _command_line, _command_arg):
        super(PersistentServerConnectionApplication, self).__init__()
        super(BaseRestUtils, self).__init__()

    def handle(self, in_bytes):
        return self.handle_wrapper(in_bytes, self.handle_func)

    def handleStream(self, handle, in_bytes):
        """
        For future use
        """
        raise NotImplementedError("PersistentServerConnectionApplication.handleStream")

    def done(self):
        """
        Virtual method which can be optionally overridden to receive a
        callback after the request completes.
        """
        pass

    def handle_func(self, request):
        # Handle a syncronous from splunkd.
        service = self.service_from_request(request, use_system_token=True)
        payload = self.get_payload(request)
        source_app_id = request["header_map"][self.SOURCE_APP_ID_KEY]
        ai_service_data_enabled, _ = self._fetch_telemetry_details(service, request, "open_in_search_telemetry")
        # Retrieve the index for the data
        if (
            "sessionId" in payload
            and "user" in payload
            and "spl" in payload
        ):
            feedback_indx = service.indexes["_internal"] # pyright: ignore
            hashed_user = hash(payload["user"])
            telemetry_string = f"session_id='{payload['sessionId']}', source_app={source_app_id}, chat_id={getattr(payload, 'chat_id', None)}, user='{hashed_user}'"
            if ai_service_data_enabled:
                telemetry_string += f", spl='{payload['spl']}'"
            feedback_indx.submit(
                telemetry_string,
                sourcetype="splgen_openinsearch",
                host="local",
            )
            return self.create_response({"message": "success"}, 201)

        return self.create_response({"error": "Incorrect arguments provided"}, 400)
