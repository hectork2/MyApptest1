import logging
import re
import requests
import json
from requests import HTTPError
import traceback

from splunklib.binding import HTTPError as SplunkHTTPError
from splunklib.client import Service
from ..metadata.collection import MetadataCollection
from ..constants import FEATURE_FLAG_KEY_CUSTOMIZATION, DATA_ID_SOURCETYPE_METADATA, DATA_ID_USER_SEARCH_LOGS, DATA_ID_INDEXED_FIELDS
from ..utils import (
    log_kwargs,
    read_splk_content,
    SimpleError,
    get_updated_feature_settings,
    get_enabled_feature_flags,
    update_saved_searches_from_feature_settings,
    get_deployment_id,
    get_app_version,
    store_secret_key,
    get_secret_key
)
from ..permission_utils import build_permissions_obj
from ..scs_utils import ScsUtils
from ..utils.cloud_connected.proxy_settings_utils import ProxySettingsUtils
from ..cloud_connected.cc_configurations.collection import CloudConnectedConfigurationsCollection

FEATURE_FLAG_PERMISSIONS_MAP = {FEATURE_FLAG_KEY_CUSTOMIZATION: build_permissions_obj}

HTTP_METHOD_POST = "POST"
HTTP_METHOD_DELETE = "DELETE"
HTTP_METHOD_GET = "GET"

PATH_INFERENCE = "api/inference"
PATH_RETRIEVAL = "api/retrieval"
PATH_TOOLS = "api/tools"
PATH_SEARCH = "api/search"
PATH_METADATA = "api/metadata"
PATH_DATA_STATUS = "data/status"
PATH_DATA_UPLOAD = "data/upload"

# Model Constants
LLAMA31_INSTRUCT = "llama31_instruct"
GPT_4O = "gpt-4o"

class SaiaApi:
    FEATURE_FLAG_HEADER_KEY = "X-Feature-Flags"

    def __init__(self, service: Service, system_scoped_service: Service, username: str, chat_id: str = "none", hashed_user: int = -1):
        self.service: Service = service
        self.system_scoped_service: Service = system_scoped_service
        self.username: str = username
        self.logger = logging.getLogger(self.__class__.__name__)
        ScsUtils.set_logger(self.logger)
        self.tenant_info = self.get_tenant_info() if ScsUtils.is_cloud_stack(self.system_scoped_service.token) else self.get_tenant_info_for_cmp_stack()
        self.api_version = "v1alpha1"
        self.metadata_collection = MetadataCollection(service)
        self.chat_id = chat_id
        self.hashed_user = hashed_user
        self.deployment_id = get_deployment_id(system_scoped_service)
        self.app_version = get_app_version(system_scoped_service)
        ProxySettingsUtils.set_logger(self.logger)

    def get_scs_token(self):
        try:
            res = self.system_scoped_service.get(
                "/services/authorization/scs_tokens",
                principalId="saia",
                scope="tenant",
                output_mode="json",
            )

            content = read_splk_content(res)
            try:
                scs_token = content.get("scs_token")
            except KeyError:
                raise RuntimeError({"status": 403, "error": "No SCS token found"})
        except SplunkHTTPError as e:
            if e.status == 400 or e.status == 403 or e.status == 404:
                self.logger.error(
                    log_kwargs(
                        message=getattr(e, 'message', e),
                        chat_id=self.chat_id,
                        user=self.hashed_user,
                        deployment_id=self.deployment_id,
                        saia_app_version=self.app_version
                    )
                )
                scs_token = self.get_secret("api_key")
            else:
                self.logger.error(
                    log_kwargs(
                        message=getattr(e, 'message', e),
                        chat_id=self.chat_id,
                        user=self.hashed_user,
                        deployment_id=self.deployment_id,
                        saia_app_version=self.app_version
                    )
                )
                raise e

        self.logger.info(
            log_kwargs(
                message="Fetched SCS token.",
                chat_id=self.chat_id,
                user=self.hashed_user,
                deployment_id=self.deployment_id,
                saia_app_version=self.app_version
            )
        )
        return scs_token

    def get_secret(self, secret_key, required=True):
        try:
            secret = self.system_scoped_service.storage_passwords[f"{secret_key}:{self.username}:"]
            return secret.clear_password
        except KeyError:
            if required:
                # Special case, if api_key failed fetching
                # probably needs EC -> SCS commerce provisioning
                if secret_key == "api_key":
                    error = "Service not initialized, please contact support."
                else:
                    # Surface generic error
                    error = f"unable to fetch secret '{secret_key}'"
                raise RuntimeError(
                    {
                        "status": 403,
                        "error": error,
                    }
                )
            else:
                return None

    def get_tenant_info(self):
        # example content of entry.content
        # {
        #     "cloudStack": "some-fancy-stack",
        #     "eai:acl": null,
        #     "ecRegion": "region-iad10",
        #     "globalHostname": "api.staging.scs.splunk.com",
        #     "legacyAppHostname": "app.staging.scs.splunk.com",
        #     "regionHostname": "region-iad10.api.staging.scs.splunk.com",
        #     "tenant": "some-fancy-tenant",
        #     "tenantHostname": "some-fancy-tenant.api.staging.scs.splunk.com"
        # }
        try:
            res = self.system_scoped_service.get(
                "/services/server/scs/tenantinfo", output_mode="json"
            )
            tenant_info = read_splk_content(res)
            if tenant_info["tenantHostname"].endswith("playgound.scs.splunk.com"):
                if self.get_secret("tenant") is not None:
                    tenant_info["tenant"]
        except SplunkHTTPError as e:
            if e.status == 404:
                tenant_info = {
                    "tenantHostname": self.get_secret("tenant_hostname"),
                    "tenant": self.get_secret("tenant"),
                }
            else:
                self.logger.error(
                    log_kwargs(
                        message=getattr(e, 'message', e),
                        chat_id=self.chat_id,
                        user=self.hashed_user,
                        deployment_id=self.deployment_id,
                        saia_app_version=self.app_version
                    )
                )
                raise e

        return tenant_info

    def get_tenant_info_for_cmp_stack(self):
        try:
            configs = ScsUtils.fetch_scs_configs(self.system_scoped_service, self.system_scoped_service.token)
        except Exception as e:
            error = f"Unable to fetch tenant info. Exception: {str(e)}"
            self.logger.error(error)

        return {
            "tenant": configs[CloudConnectedConfigurationsCollection.SAIA_TENANT_NAME],
            "tenantHostname": configs[CloudConnectedConfigurationsCollection.SAIA_TENANT_HOSTNAME],
        }

    def _construct_feature_flags_header(self, enabled_feature_flags):
        flag_configs = []
        for flag in enabled_feature_flags:
            config = {"id": flag}
            if flag in FEATURE_FLAG_PERMISSIONS_MAP:
                config["permissions"] = FEATURE_FLAG_PERMISSIONS_MAP[flag](
                    self.service, self.username, self.logger
                )
            flag_configs.append(config)

        return json.dumps(flag_configs)

    def _handle_feature_setting_mismatch(self, feature_settings, enabled_feature_flags, remote_enabled_features):
        new_feature_settings = get_updated_feature_settings(feature_settings, remote_enabled_features)
        self.metadata_collection.update({MetadataCollection.KEY_FEATURE_SETTINGS: new_feature_settings})
        update_saved_searches_from_feature_settings(new_feature_settings, self.service)
        enabled_feature_flags = get_enabled_feature_flags(new_feature_settings)
        return enabled_feature_flags

    def _make_request(self, method, path, body=None, headers=None, stream=False, request_id=None):
        base_url = self.tenant_info["tenantHostname"]
        if base_url and not base_url.startswith("http"):
            base_url = f"https://{base_url}"
        url = f"{base_url}/{self.tenant_info['tenant']}/saia-api/{self.api_version}/{path}"
        self.logger.info(
            log_kwargs(
                message=f"sending request to: {url}",
                chat_id=self.chat_id,
                user=self.hashed_user,
                deployment_id=self.deployment_id,
                saia_app_version=self.app_version,
                tenant_id=self.tenant_info["tenant"],
                request_id=request_id
            )
        )
        if ScsUtils.is_cloud_stack(self.system_scoped_service.token):
            api_key = self.get_scs_token()
        else:
            api_key = ScsUtils.get_scs_token_for_cmp_stack(self.system_scoped_service)
        if not headers:
            headers = {}
        headers["authorization"] = f"Bearer {api_key}"

        proxies = ProxySettingsUtils.fetch_proxies_if_enabled(self.system_scoped_service)
        res = requests.request(method, url, headers=headers, json=body, stream=stream, proxies=proxies)
        self.logger.info(
            log_kwargs(
                status=res.status_code,
                chat_id=self.chat_id,
                user=self.hashed_user,
                deployment_id=self.deployment_id,
                saia_app_version=self.app_version,
                tenant_id=self.tenant_info["tenant"],
                request_id=request_id
            )
        )
        res.raise_for_status()
        return res

    def _make_get_request(self, path, request_id=None, headers=None):
        return self._make_request(HTTP_METHOD_GET, path, headers=headers, request_id=request_id)

    def _make_post_request(self, path, request_id, body, headers=None, stream=False):
        if not headers:
            headers = {}

        enabled_feature_flags, feature_settings = self.metadata_collection.get_enabled_feature_flags()
        headers[self.FEATURE_FLAG_HEADER_KEY] = self._construct_feature_flags_header(
            enabled_feature_flags
        )

        def request_func(headers):
            return self._make_request(HTTP_METHOD_POST, path, body, headers, stream, request_id=request_id)

        try:
            res = request_func(headers)
        except HTTPError as e:
            self.logger.error(
                log_kwargs(
                    error_traceback=traceback.format_exc()
                )
            )
            if e.response.status_code == 403:
                error_body = e.response.json()
                error_content = error_body.get("errors", [{}])[0]
                if error_content and error_content.get("error_id") == "mismatch_features_error":
                    self.logger.info(
                        log_kwargs(
                            message="Feature settings mismatch between app and service, updating settings.",
                            chat_id=self.chat_id,
                            user=self.hashed_user,
                            deployment_id=self.deployment_id,
                            saia_app_version=self.app_version
                        )
                    )
                    adjusted_enabled_flags = self._handle_feature_setting_mismatch(feature_settings, enabled_feature_flags, error_content["enabled_features"])

                    if (path == PATH_SEARCH):
                        # Want to retry the request with updated features header for search
                        self.logger.info(
                            log_kwargs(
                                message="Retrying search request",
                                chat_id=self.chat_id,
                                user=self.hashed_user,
                                deployment_id=self.deployment_id,
                                saia_app_version=self.app_version
                            )
                        )
                        headers[self.FEATURE_FLAG_HEADER_KEY] = self._construct_feature_flags_header(adjusted_enabled_flags)
                        res = request_func(headers)
                        res.headers["notify-reload-required"] = True
                        return res
            raise e

        return res

    def _make_delete_request(self, path, headers=None):
        return self._make_request(HTTP_METHOD_DELETE, path, headers=headers)

    def _make_data_upload_request(self, request_id, data_id, payload):
        body = {
            "data_id": data_id,
            "data": payload,
            "request_id": request_id,
            "deployment_id": self.deployment_id,
            "app_version": self.app_version,
        }
        res = self._make_post_request(PATH_DATA_UPLOAD,request_id, body)
        return res

    def update_data_status(self, data_id, set_for_deletion, request_id):
        body = {
            "data_id": data_id,
            "enable_delete": set_for_deletion,
            "deployment_id": self.deployment_id,
            "app_version": self.app_version,
            "request_id": request_id,
        }
        res = self._make_post_request(PATH_DATA_STATUS,request_id=request_id, body=body)
        if res.status_code == 200:
            return res.json().get("should_upload")
        else:
            return False

    def get_tool(
        self,
        job_id,
        tools,
        chat_history,
        locale,
        log_to_telemetry,
    ):
        return self._make_post_request(
            PATH_TOOLS,
            request_id=job_id,
            body={
                "chat_history": chat_history,
                "locale": locale,
                "tools": tools,
                "log_to_telemetry": log_to_telemetry,
                "request_id": job_id
            }
        )

    def inference(
        self,
        job_id,
        system_prompt,
        chat_history,
        log_to_telemetry,
        source_app_id,
    ):
        return self._make_post_request(
            PATH_INFERENCE,
            request_id=job_id,
            body={
                "system_prompt": system_prompt,
                "chat_history": chat_history,
                "log_to_telemetry": log_to_telemetry,
                "request_id": job_id,
                "source_app_id": source_app_id,
                "deployment_id": self.deployment_id,
                "app_version": self.app_version,
                "chat_id": self.chat_id,
                "user": self.hashed_user,
            },
            headers={"X-Request-Id": job_id},
            stream=True,
        )

    def retrieval(
        self,
        job_id,
        collection,
        k,
        threshold,
        metadata_fields,
        query,
        log_to_telemetry,
        source_app_id,
    ):
        return self._make_post_request(
            PATH_RETRIEVAL,
            request_id=job_id,
            body={
                "collection": collection,
                "k": k,
                "threshold": threshold,
                "metadata_fields": metadata_fields,
                "query": query,
                "log_to_telemetry": log_to_telemetry,
                "request_id": job_id,
                "source_app_id": source_app_id,
                "deployment_id": self.deployment_id,
                "app_version": self.app_version,
                "chat_id": self.chat_id,
                "user": self.hashed_user,
            },
            headers={"X-Request-Id": job_id},
        )

    def make_orchestration_request(
        self,
        job_id,
        user_prompt,
        chat_history,
        classification,
        locale,
        log_to_telemetry,
        was_chat_empty,
        source_app_id,
        ast,
        rag_data_only,
        rewrite_content,
    ):

        return self._make_post_request(
            PATH_SEARCH,
            request_id=job_id,
            body={
                "user_prompt": user_prompt,
                "chat_history": chat_history,
                "classification": classification,
                "locale": locale,
                "log_to_telemetry": log_to_telemetry,
                "request_id": job_id,
                "chat_id": self.chat_id,
                "user": self.hashed_user,
                "deployment_id": self.deployment_id,
                "app_version": self.app_version,
                "was_chat_empty": was_chat_empty,
                "source_app_id": source_app_id,
                "ast": ast,
                "rag_data_only": rag_data_only,
                "rewrite_content": rewrite_content,
                "orchestration_request": True,
                # "use_state_streamer": True, # Need to set if we want UI Tools
            },
            headers={"X-Request-Id": job_id},
        )

    def search(
        self,
        job_id,
        user_prompt,
        chat_history,
        classification,
        locale,
        log_to_telemetry,
        was_chat_empty,
        source_app_id,
        ast,
        rag_data_only,
        rewrite_content,
        indexed_fields,
        use_state_streamer,
    ):

        return self._make_post_request(
            PATH_SEARCH,
            request_id=job_id,
            body={
                "user_prompt": user_prompt,
                "chat_history": chat_history,
                "classification": classification,
                "locale": locale,
                "log_to_telemetry": log_to_telemetry,
                "request_id": job_id,
                "chat_id": self.chat_id,
                "user": self.hashed_user,
                "deployment_id": self.deployment_id,
                "app_version": self.app_version,
                "was_chat_empty": was_chat_empty,
                "source_app_id": source_app_id,
                "ast": ast,
                "rag_data_only": rag_data_only,
                "rewrite_content": rewrite_content,
                # "use_state_streamer": True, # Need to set this if we want UI Tools
                "indexed_fields": indexed_fields,
                "use_state_streamer": use_state_streamer,
            },
            headers={"X-Request-Id": job_id},
            stream=True,
        )

    def parser_retry(self, spl, error, job_id, locale, log_to_telemetry, source_app_id):
        return self._make_post_request(
            PATH_SEARCH,
            request_id=job_id,
            body={
                "fix_spl": True,
                "last_err": error,
                "last_spl": spl,
                "rewrite_content": True,
                "classification": 4,
                "locale": locale,
                "log_to_telemetry": log_to_telemetry,
                "request_id": job_id,
                "chat_id": self.chat_id,
                "user": self.hashed_user,
                "deployment_id": self.deployment_id,
                "app_version": self.app_version,
                "source_app_id": source_app_id,
            },
            headers={"X-Request-Id": job_id},
            stream=True,
        )

    def optimize_spl(
        self,
        job_id,
        user_prompt,
        locale,
        log_to_telemetry,
        source_app_id,
        rewrite_content
    ):

        return self._make_post_request(
            PATH_SEARCH,
            request_id=job_id,
            body={
                "user_prompt": user_prompt,
                "chat_history": '[{"role": "user", "content": ' + json.dumps(user_prompt) + '}]',
                "classification": "3",
                "locale": locale,
                "log_to_telemetry": log_to_telemetry,
                "request_id": job_id,
                "chat_id": self.chat_id,
                "user": self.hashed_user,
                "deployment_id": self.deployment_id,
                "app_version": self.app_version,
                "source_app_id": source_app_id,
                "rewrite_content": rewrite_content,
            },
            headers={"X-Request-Id": job_id},
            stream=True,
        )

    def metadata(self, request_id):
        return self._make_get_request(
            f"{PATH_METADATA}?version={self.app_version}",
            request_id=request_id,
            headers={"X-Request-ID": request_id},
        )

    def metering(self, request_id):
        return self._make_get_request(
            f"{PATH_METADATA}?throttling_data_only=true&version={self.app_version}",
            request_id=request_id,
            headers={"X-Request-ID": request_id}
        )

    def stop_generation(self, request_id):
        return self._make_post_request(
            PATH_SEARCH,
            request_id=request_id,
            body={
                "id_to_cancel": request_id
            }
        )

    def submit_sourcetype_metadata(self, request_id, field_data):
        return self._make_data_upload_request(
            request_id,
            DATA_ID_SOURCETYPE_METADATA,
            field_data,
        )

    def submit_user_search_logs(self, request_id, searches):
        return self._make_data_upload_request(request_id, DATA_ID_USER_SEARCH_LOGS, searches)
