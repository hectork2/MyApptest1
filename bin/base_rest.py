import sys
import os
import json
import logging
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from splunklib.client import connect
from splunklib.binding import handler
from splunklib.binding import HTTPError
from splunklib.searchcommands import environment

class BaseRestUtils:
    API_VERSION = "1.0.0"
    CONFIG_FILE_ENDPOINT = "configs/conf-splunkaiassistant/splunk_ai_assistant"
    # Browser behaviour can be inconsistent, so lowercase both this const as well as header keys
    # inside request["header_map"]
    SOURCE_APP_ID_KEY = "Source-App-ID".lower()
    
    SOURCE_APP_ID_KEY_SNAKE_CASE = "Source_App_ID".lower()

    def __init__(self):
        environment.app_root = os.path.join(os.path.dirname(__file__), "..")
        logger, _ = environment.configure_logging(self.__class__.__name__)
        if logger:
            self.logger = logger

    def _fetch_telemetry_details(self, service, request, handler_type=None):
        try:
            config_response = service.get("config", owner="nobody", app=request["ns"]["app"], output_mode="json", headers=[(self.SOURCE_APP_ID_KEY, "internal")])
            config = self.decode_config_response_body(config_response)

            feature_settings = config["feature_settings"]
            enabled_features = [key for key, val in feature_settings.items() if val["enabled"] is True]

            return config.get("ai_service_data_enabled") == "1" and not (config.get("permanently_disable_ai_service_data") == "1"), enabled_features
        except:
            # telemetry shouldn't block functionality
            self.logger.info(f"Fetching telemetry details for {handler_type} handler")
            return False, []

    def validate_payload(self, payload, required_fields):
        for field in required_fields:
            if field not in payload:
                raise RuntimeError(
                    {"error": f"'{field}' missing in the payload", "status": 400}
                )

    def get_payload(self, request):
        try:
            return json.loads(request.get("payload", "{}"))
        except json.JSONDecodeError:
            raise RuntimeError(
                {"error": "request payload is not valid JSON", "status": 400}
            )

    def get_query_params(self, request):
        query = request["query"]
        query_params = {}
        for key, val in query:
            if val.lower() in ["true", "false"]:
                val = val.lower() == "true"
            query_params[key] = val

        return query_params

    def get_header_map(self, request):
        headers = request["headers"]
        header_map = defaultdict(list)
        for header_items in headers:
            key = header_items[0].lower()
            # Value should be a list only if there are multiple values for key
            header_map[key] = header_items[1:] if len(header_items) > 2 else header_items[1]

        return header_map

    def handle_wrapper(self, in_bytes, handle_func, require_source_app_id=True):
        """
        Called for a simple synchronous request.
        @param in_bytes: request data passed in
        @rtype: string or dict
        @return: String to return in response.  If a dict was passed in,
                 it will automatically be JSON encoded before being returned.
        """

        try:
            try:
                request = json.loads(in_bytes.decode("utf-8"))
                request["header_map"] = self.get_header_map(request)
                if require_source_app_id and self.SOURCE_APP_ID_KEY not in request["header_map"] or request["header_map"][self.SOURCE_APP_ID_KEY] == []:
                    query_params = self.get_query_params(request)
                    if self.SOURCE_APP_ID_KEY in query_params:
                        request["header_map"][self.SOURCE_APP_ID_KEY] = query_params[self.SOURCE_APP_ID_KEY]
                    elif self.SOURCE_APP_ID_KEY_SNAKE_CASE in query_params:
                        request["header_map"][self.SOURCE_APP_ID_KEY] = query_params[self.SOURCE_APP_ID_KEY_SNAKE_CASE]
                    else:
                        return self.create_response({"header_map": request["header_map"], "headers": request["headers"], "error": f"Missing required {self.SOURCE_APP_ID_KEY} header"}, 400)

                return handle_func(request)
            except json.JSONDecodeError as e:
                self.logger.error(e)
                return self.create_response(
                    payload={"error": "JSON decoding error"}, status_code=400
                )
            except RuntimeError as e:
                (content,) = e.args
                if isinstance(content, str):
                    return self.create_response(
                        payload={"error": content}, status_code=500
                    )
                elif isinstance(content, dict):
                    return self.create_response(
                        payload={"error": content["error"]},
                        status_code=content["status"],
                    )
                else:
                    return self.create_response(
                        payload={"error": "unknown error"}, status_code=500
                    )
        except Exception as e:
            logging.exception("unknown exception")
            self.logger.error(e)
            return self.create_response(payload={"error": str(e)}, status_code=500)

    def service_from_request(self, request, use_system_token=False):
        session = request["session"]
        auth_token = session["authtoken"]
        owner = session["user"]
        if use_system_token:
            if "system_authtoken" in request:
                auth_token = request["system_authtoken"]
                owner = "splunk-system-user"
            else:
                self.logger.error(
                    f"system token not available for REST handler {self.__class__.__name__}"
                )
        try:
            return connect(
                token=auth_token,
                handler=handler(timeout=1),
                host="127.0.0.1",
                app=request["ns"]["app"],
                owner=owner,
                retries=10,
                retryDelay=2
            )
        except HTTPError as e:
            self.logger.error(e)
            raise e

    def decode_config_response_body(self, response):
        response_body = response["body"].read()
        return json.loads(response_body.decode("utf-8"))

    def create_response(self, payload, status_code):
        return {"payload": payload, "status": status_code}
