# Copyright 2024 Splunk Inc.
import json
import logging
import requests
import sys
import os
import uuid
from datetime import timezone, datetime

from splunk.persistconn.application import PersistentServerConnectionApplication

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from base_rest import BaseRestUtils

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from spl_gen.utils import log_kwargs, fetch_splunk_license, get_secret_key, store_secret_key, reload_saia_app
from spl_gen.scs_utils import ScsUtils
from spl_gen.utils.cloud_connected.conf_utils import ConfUtils
from spl_gen.cloud_connected.cc_configurations.collection import CloudConnectedConfigurationsCollection

from spl_gen.constants import SAIA_REALM, SAIA_SCS_STANZA, SAIA_CMP_ONBOARD_OTP_LENGTH, SAIA_CONF_FILE
from spl_gen.constants import SAIA_PRIVATE_KEY, SAIA_PUBLIC_JWK, SCS_DOMAIN
from spl_gen.constants import SAIA_CONFIGURATION, SAIA_CONFIGURATION_FILE, SAIA_CONFIGURATION_STANZA

from spl_gen.errors import messages

class CompleteOnboardingHandler(PersistentServerConnectionApplication, BaseRestUtils):

    def __init__(self, _command_line, _command_arg):
        super(PersistentServerConnectionApplication, self).__init__()
        super(BaseRestUtils, self).__init__()
        self.logger = logging.getLogger(self.__class__.__name__)
        ScsUtils.set_logger(self.logger)
        ConfUtils.set_logger(self.logger)

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
        system_scoped_service = self.service_from_request(request, use_system_token=True)
        system_authtoken = request['system_authtoken']
        session_authtoken = request['session']['authtoken']

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

        activation_code = payload["activation_code"]

        #decode the activation code
        try:
            decoded_activation_code = ScsUtils.base64url_decode(activation_code)
        except Exception as e:
            self.logger.error(f"Error decoding activation code : {str(e)}")
            return self.create_response({"error": messages.INVALID_ACTIVATION_TOKEN}, 400)

        if not decoded_activation_code:
            self.logger.error(messages.EMPTY_ACTIVATION_TOKEN)
            return self.create_response({"error": messages.EMPTY_ACTIVATION_TOKEN}, 400)

        tenant_hostname, service_principal, kid =  decoded_activation_code.split("::")

        if '.api.' not in tenant_hostname or not tenant_hostname.endswith(f".{SCS_DOMAIN}"):
            self.logger.error(messages.TENANT_HOSTNAME_CHECK_ERROR)
            return self.create_response({"error": messages.TENANT_HOSTNAME_CHECK_ERROR}, 400)

        try:
            public_jwk = get_secret_key(system_scoped_service, SAIA_PUBLIC_JWK, SAIA_REALM, True)
            public_jwk_kid = ScsUtils.get_key_id_from_public_key(public_jwk)

            if kid != public_jwk_kid:
                self.logger.error("Invalid activation code provided, kid does not match public JWK")
                return self.create_response({"error": messages.INVALID_ACTIVATION_TOKEN}, 400)

        except Exception as e:
            self.logger.error(str(e))
            return self.create_response({"error": f"{messages.UNABLE_TO_FETCH_PUBLIC_KEY}. {messages.CONTACT_SPLUNK_SUPPORT}"}, 500)

        scs_region = ScsUtils.fetch_scs_region(system_scoped_service, system_authtoken)
        tenant_name, scs_environment = ScsUtils.fetch_tenant_and_env_from_tenant_hostname(tenant_hostname)

        try:
            private_key_text = get_secret_key(system_scoped_service, SAIA_PRIVATE_KEY, SAIA_REALM)

            scs_token, scs_token_expiry = ScsUtils.generate_scs_token(service_principal, private_key_text, public_jwk_kid, scs_environment, tenant_name, scs_region, system_scoped_service)
            ScsUtils.validate_access_token(scs_token, tenant_name, tenant_hostname, system_scoped_service)
        except requests.exceptions.ProxyError as e:
            self.logger.error(
                log_kwargs(
                    message=f"Proxy error: {str(e)}",
                    tenant_id=tenant_name
                )
            )
            return self.create_response({"error": f"{messages.INVALID_PROXY_SETTINGS}"}, 400)
        except requests.exceptions.ConnectionError as e:
            self.logger.error(
                log_kwargs(
                    message=f"Connection error: {str(e)}",
                    tenant_id=tenant_name
                )
            )
            return self.create_response({"error": f"{messages.CANNOT_CONNECT_TO_SPLUNK_CLOUD}"}, 400)
        except Exception as e:
            self.logger.error(
                log_kwargs(
                    message=f"Error while token generation / validation : {str(e)}",
                    tenant_id=tenant_name
                )
            )
            return self.create_response({"error": f"{str(e)}"}, 500)

        self.logger.info(
            log_kwargs(
                scs_token=scs_token,
                UUID=logging_uuid,
                user=hashed_user,
                tenant_hostname=tenant_hostname,
                tenant_id=tenant_name,
                service_principal=service_principal
            )
        )

        try:
            ScsUtils.test_saia_search(tenant_hostname, tenant_name, scs_token, system_scoped_service)
            self.logger.info(
                log_kwargs(
                    message=f"Test SAIA search successful",
                    tenant_id=tenant_name
                )
            )
        except Exception as e:
            self.logger.error(
                log_kwargs(
                    message=f"Failed to test SAIA search. Error : {str(e)}",
                    tenant_id=tenant_name
                )
            )
            return self.create_response({"error": f"{messages.SCS_TEST_CONNECTION_FAILURE}. {messages.CONTACT_SPLUNK_SUPPORT}"}, 500)

        cc_configuration_collection_object = CloudConnectedConfigurationsCollection(system_scoped_service)

        configs = {
            CloudConnectedConfigurationsCollection.SAIA_TENANT_NAME: tenant_name,
            CloudConnectedConfigurationsCollection.SAIA_TENANT_HOSTNAME: tenant_hostname,
            CloudConnectedConfigurationsCollection.SAIA_SERVICE_PRINCIPAL: service_principal,
            CloudConnectedConfigurationsCollection.SAIA_SCS_TOKEN_EXPIRY: scs_token_expiry,
            CloudConnectedConfigurationsCollection.SAIA_SCS_TOKEN: scs_token,
            CloudConnectedConfigurationsCollection.SAIA_LAST_SETUP_TIMESTAMP: int((datetime.now(timezone.utc)).timestamp())
        }
        try:
            cc_configuration_collection_object.update(configs)

            ScsUtils.cron_for_key_rotation('enable', system_scoped_service)
        except Exception as e:
            self.logger.error(str(e))
            return self.create_response(
                {"error": f"{str(e)}. {messages.CONTACT_SPLUNK_SUPPORT}"}, 500)

        try:
            ScsUtils.update_app_configured(system_scoped_service, system_authtoken, session_authtoken, True)
        except Exception as e:
            self.logger.error(str(e))
            return self.create_response({"error": f"{str(e)}"}, 500)

        if not reload_saia_app(system_scoped_service):
            self.logger.error(messages.FAILED_RELOADING_APP)
            return self.create_response(
                {"error": f"{messages.FAILED_RELOADING_APP}. {messages.CONTACT_SPLUNK_SUPPORT}"}, 500)

        self.logger.info(
            log_kwargs(
                tenant_id=tenant_name,
                success="200"
            )
        )

        return {"payload": json.dumps({
            "tenant_name": tenant_name,
            "tenant_hostname": tenant_hostname
        }), "status": 200}
