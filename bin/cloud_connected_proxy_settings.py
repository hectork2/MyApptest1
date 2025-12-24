# Copyright 2024 Splunk Inc.
import json
import logging
import sys
import os
import uuid

from splunk.persistconn.application import PersistentServerConnectionApplication

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from base_rest import BaseRestUtils

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from spl_gen.utils import log_kwargs, fetch_splunk_license, get_secret_key, store_secret_key, reload_saia_app
from spl_gen.scs_utils import ScsUtils
from spl_gen.utils.cloud_connected.proxy_settings_utils import ProxySettingsUtils
from spl_gen.errors import messages

class CloudConnectedProxySettingsHandler(PersistentServerConnectionApplication, BaseRestUtils):

    def __init__(self, _command_line, _command_arg):
        super(PersistentServerConnectionApplication, self).__init__()
        super(BaseRestUtils, self).__init__()
        self.logger = logging.getLogger(self.__class__.__name__)
        ScsUtils.set_logger(self.logger)
        ProxySettingsUtils.set_logger(self.logger)

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

    def handle_post_request(self, system_scoped_service, payload):
        proxy_settings = payload.get("proxy_settings")
        if proxy_settings is None:
            return self.create_response({"error": messages.PROXY_SETTINGS_NOT_PROVIDED}, 400)

        valid, proxy_auth_present = ProxySettingsUtils.is_valid_proxy_settings_and_auth_present(proxy_settings)
        if not valid:
            return self.create_response({"error": messages.INVALID_PROXY_SETTINGS}, 400)

        ProxySettingsUtils.store_proxy_settings(system_scoped_service, proxy_settings, proxy_auth_present)

        self.logger.info(
            log_kwargs(
                success="200"
            )
        )
        return {"payload": json.dumps({"status": "success"}), "status": 200}

    def handle_get_request(self, system_scoped_service):
        try:
            proxy_settings = ProxySettingsUtils.fetch_all_proxy_settings(system_scoped_service)
            return {"payload": json.dumps({'proxy_settings': proxy_settings}), "status": 200}
        except Exception as e:
            self.logger.error(f'{messages.ERROR_FETCHING_PROXY_SETTINGS} : {str(e)}')
            return self.create_response({"error": messages.ERROR_FETCHING_PROXY_SETTINGS}, 500)

    def handle_delete_request(self, system_scoped_service):
        try:
            ProxySettingsUtils.clear_proxy_settings(system_scoped_service)

            return {"payload": json.dumps({"status": "success"}), "status": 200}
        except Exception as e:
            self.logger.error(f'{messages.ERROR_CLEARING_PROXY_SETTINGS} : {str(e)}')
            return self.create_response({"error": messages.ERROR_CLEARING_PROXY_SETTINGS}, 500)

    def handle_func(self, request):
        system_scoped_service = self.service_from_request(request, use_system_token=True)
        system_authtoken = request['system_authtoken']

        if ScsUtils.is_cloud_stack(system_authtoken):
            return self.create_response({"error": messages.OPERATION_NOT_ALLOWED_CLOUD_STACK}, 400)

        hashed_user = hash(request["session"]["user"])
        logging_uuid = str(uuid.uuid4())
        payload = self.get_payload(request)
        self.logger.info(
            log_kwargs(
                UUID=logging_uuid,
                user=hashed_user,
                request_payload=payload,
            )
        )

        if request["method"] == "POST":
            return self.handle_post_request(system_scoped_service, payload)
        elif request["method"] == "GET":
            return self.handle_get_request(system_scoped_service)
        elif request["method"] == "DELETE":
            return self.handle_delete_request(system_scoped_service)

        return self.create_response({"error": "Something went wrong"}, 500)
