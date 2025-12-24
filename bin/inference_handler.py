# Copyright 2024 Splunk Inc.
import json
import sys
import uuid
import os
import copy
import time

from splunk.persistconn.application import PersistentServerConnectionApplication

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from base_rest import BaseRestUtils

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from spl_gen.remote.jobs import AsyncHttpJobs
from spl_gen.remote.v1alpha1 import SaiaApi
from spl_gen.saia_tool_scratchpad import SaiaToolScratchpad
from spl_gen.utils import get_app_version

class InferenceHandler(PersistentServerConnectionApplication, BaseRestUtils):
    jobs = AsyncHttpJobs()

    def __init__(self, _command_line, _command_arg):
        super(PersistentServerConnectionApplication, self).__init__()
        super(BaseRestUtils, self).__init__()

    def _clean_scratchpad(self, saia_tool_scratchpad, job_id):
        saia_tool_scratchpad.clean(job_id)

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

    def handle_get_request(self, service, system_scoped_service, query_params, saia_tool_scratchpad, logging_context):
        # Fetch content of scratchpad for this tool call
        if "job_id" in query_params:
            result = saia_tool_scratchpad.get(query_params["job_id"])
            # As a side effect of tool call, we clean the scratchpad for this user
            self._clean_scratchpad(saia_tool_scratchpad=saia_tool_scratchpad, job_id=query_params["job_id"])
            return self.create_response({"result": result}, 200)
        return self.create_response({"error": "Incorrect arguments provided"}, 400)

    def handle_post_request(self, service, system_scoped_service, payload, saia_tool_scratchpad, logging_context):

        if "chat_history" not in payload or "system_prompt" not in payload:
           return self.create_response({"error": "Incorrect arguments provided"}, 400)

        if "chat_id" in payload:
            chat_id = payload["chat_id"]
        else:
            chat_id = "oneshot"

        # Serialize chat history as a string if it's an object
        if isinstance(payload["chat_history"], str):
            chat_history = payload["chat_history"]
        else:
            chat_history = json.dumps(payload["chat_history"])

        if "stream" in payload and payload["stream"] == True:
            # Initiate a tool call with provided tool_id
                job_id = str(uuid.uuid4())
                job_id = self.jobs.create_from_request(
                    app_name=logging_context["app_name"],
                    user=logging_context["user"],
                    hashed_user=logging_context["hashed_user"],
                    kwargs={
                        "chat_history": chat_history,
                        "system_prompt": payload["system_prompt"]
                    },
                    service=service,
                    system_scoped_service=system_scoped_service,
                    chat_id=chat_id,
                    response_id=job_id,
                    app_version=logging_context["saia_app_version"],
                    should_log_telemetry=logging_context["should_log_telemetry"],
                    source_app_id=logging_context["source_app_id"],
                    tool_id="inference"
                )
                return self.create_response({"job_id": job_id}, 200)
        else:
            # make non-streaming call, block until content all returned
            job_id = str(uuid.uuid4())
            saia_api = SaiaApi(
                service,
                system_scoped_service,
                logging_context["user"],
                chat_id,
                logging_context["hashed_user"]
            )
            res = saia_api.inference(
                job_id,
                payload["system_prompt"],
                chat_history,
                logging_context["should_log_telemetry"],
                logging_context["source_app_id"]
            )
            return self.create_response(json.dumps({"response": res.text}), 200)

    def handle_func(self, request):
        # Handle a syncronous from splunkd.
        service = self.service_from_request(request)
        system_scoped_service = self.service_from_request(request, use_system_token=True)
        session = request["session"]
        logging_uuid = str(uuid.uuid4())
        hashed_user = hash(session["user"])
        app_version = get_app_version(system_scoped_service)
        source_app_id = request["header_map"][self.SOURCE_APP_ID_KEY]
        query_params = self.get_query_params(request)
        payload = self.get_payload(request)
        should_log_telemetry, enabled_features = self._fetch_telemetry_details(service, request)
        ns = request["ns"]
        logging_context = dict(
            app_name=ns['app'],
            request_id=logging_uuid,
            user=session["user"],
            hashed_user=hashed_user,
            source_app_id=source_app_id,
            saia_app_version=app_version,
            should_log_telemetry=should_log_telemetry,
        )

        saia_tool_scratchpad = SaiaToolScratchpad(service, session["user"])
        if request["method"] == "GET":
            return self.handle_get_request(service, system_scoped_service, query_params, saia_tool_scratchpad, logging_context)
        elif request["method"] == "POST":
            return self.handle_post_request(service, system_scoped_service, payload, saia_tool_scratchpad, logging_context)

        return self.create_response({"error": "Method not allowed."}, 425)
