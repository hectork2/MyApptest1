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
from spl_gen.saia_tool_scratchpad import SaiaToolScratchpad
from spl_gen.utils import get_app_version

class ToolUpdateHandler(PersistentServerConnectionApplication, BaseRestUtils):
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
        # Initiate a tool call with provided tool_id
        if ("tool_id" in payload and "chat_id" in payload and "chat_entry_id" in payload and "tool_kwargs" in payload):
            job_id = self.jobs.create_from_request(
                app_name=logging_context["app_name"],
                user=logging_context["user"],
                hashed_user=logging_context["hashed_user"],
                kwargs={}, #payload["tool_kwargs"],
                service=service,
                system_scoped_service=system_scoped_service,
                chat_id=payload["chat_id"],
                response_id=payload["chat_entry_id"],
                app_version=logging_context["saia_app_version"],
                should_log_telemetry=logging_context["should_log_telemetry"],
                source_app_id=logging_context["source_app_id"],
                tool_id=payload["tool_id"]
            )
            return self.create_response({"job_id": job_id}, 200)
        return self.create_response({"error": "Incorrect arguments provided"}, 400)

    def handle_delete_request(self, service, system_scoped_service, payload, saia_tool_scratchpad, logging_context):
        if "job_id" in payload:
            # Clean the tool scratchpad for this tool call
            saia_tool_scratchpad.delete(payload["job_id"])
            return self.create_response({"message": "Scratchpad entry deleted"}, 204)
        return self.create_response({"error": "Incorrect arguments provided"}, 400)

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
        elif request["method"] == "DELETE":
            return self.handle_delete_request(service, system_scoped_service, payload, saia_tool_scratchpad, logging_context)

        return self.create_response({"error": "Method not allowed."}, 425)
