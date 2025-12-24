# Copyright 2024 Splunk Inc.
# Needed to import libraries in /lib folder
import sys, os

from splunk.persistconn.application import PersistentServerConnectionApplication

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from base_rest import BaseRestUtils

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))


class SubmitFeedbackHandler(PersistentServerConnectionApplication, BaseRestUtils):

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
        # Retrieve the index for the data
        if (
            "sessionId" in payload
            and "chatId" in payload
            and "messageId" in payload
            and "correct" in payload
            and "enabledFeatures" in payload
        ):
            # Retrieve the index for the data
            feedback_indx = service.indexes["_internal"] # pyright: ignore

            # Submit an event over HTTP
            additional_options = ""
            if "reason" in payload:
                additional_options += f",reason={payload['reason']}"
            if "explanation" in payload:
                additional_options += f",explanation='{payload['explanation']}'"
            feedback_indx.submit(
                f"session_id='{payload['sessionId']}', source_app={source_app_id}, feedback_id='{payload['messageId']}', chat_id='{payload['chatId']}', correct={payload['correct']}{additional_options}, enabled_features='{payload['enabledFeatures']}'",
                sourcetype="splgen_feedback",
                host="local",
            )
            return self.create_response({"message": "success"}, 201)

        return self.create_response({"error": "Incorrect arguments provided"}, 400)
