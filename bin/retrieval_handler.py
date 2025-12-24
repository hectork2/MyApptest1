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
from spl_gen.remote.v1alpha1 import SaiaApi
from spl_gen.utils import get_app_version

class RetrievalHandler(PersistentServerConnectionApplication, BaseRestUtils):

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

    def handle_retrieval_request(self, service, system_scoped_service, payload, logging_context):
        if "collection" not in payload or "k" not in payload or "threshold" not in payload or "metadata_fields" not in payload or "query" not in payload:
           return self.create_response({"error": "Incorrect arguments provided"}, 400)

        if "chat_id" in payload:
            chat_id = payload["chat_id"]
        else:
            chat_id = "oneshot"
        job_id = str(uuid.uuid4())
        saia_api = SaiaApi(
            service,
            system_scoped_service,
            logging_context["user"],
            chat_id,
            logging_context["hashed_user"]
        )
        res = saia_api.retrieval(
            job_id,
            payload["collection"],
            payload["k"],
            payload["threshold"],
            payload["metadata_fields"],
            payload["query"],
            logging_context["should_log_telemetry"],
            logging_context["source_app_id"]
        )
        return self.create_response(res.json(), 200)

    def handle_func(self, request):
        # Handle a syncronous from splunkd.
        service = self.service_from_request(request)
        system_scoped_service = self.service_from_request(request, use_system_token=True)
        session = request["session"]
        logging_uuid = str(uuid.uuid4())
        hashed_user = hash(session["user"])
        app_version = get_app_version(system_scoped_service)
        source_app_id = request["header_map"][self.SOURCE_APP_ID_KEY]
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
        if request["method"] == "POST":
            return self.handle_retrieval_request(service, system_scoped_service, payload, logging_context)
        else:
            return self.create_response({"error": "Method not allowed."}, 425)
