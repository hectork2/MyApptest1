# Copyright 2024 Splunk Inc.
import json
import sys
import os

from splunk.persistconn.application import PersistentServerConnectionApplication

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from base_rest import BaseRestUtils

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from spl_gen.utils import fetch_splunk_license, store_secret_key, get_secret_key, get_app_version
from spl_gen.scs_utils import ScsUtils
from spl_gen.utils.cloud_connected.conf_utils import ConfUtils
from spl_gen.cloud_connected.cc_configurations.collection import CloudConnectedConfigurationsCollection

from spl_gen.constants import SAIA_REALM, SAIA_SCS_STANZA, SAIA_CONF_FILE
from spl_gen.constants import SAIA_PRIVATE_KEY, SAIA_PUBLIC_JWK

from spl_gen.errors import messages

class SubmitOnboardingFormHandler(PersistentServerConnectionApplication, BaseRestUtils):

    def __init__(self, _command_line, _command_arg):
        super(PersistentServerConnectionApplication, self).__init__()
        super(BaseRestUtils, self).__init__()
        ScsUtils.set_logger(self.logger)

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
        saia_app_version = get_app_version(system_scoped_service)

        if ScsUtils.is_cloud_stack(system_authtoken):
            return self.create_response({"error": messages.OPERATION_NOT_ALLOWED_CLOUD_STACK}, 400)

        payload = self.get_payload(request)
        email = payload["email"]
        region = payload["region"]
        company_name = payload["company_name"]
        tenant_name = payload["tenant_name"]
        try:
            public_jwk = get_secret_key(system_scoped_service, SAIA_PUBLIC_JWK, SAIA_REALM, True)
        except Exception as e:
            self.logger.info(str(e))
            private_key_text, public_jwk, public_jwk_kid = ScsUtils.generate_key_pair()
            if not private_key_text or not public_jwk or not public_jwk:
                return self.create_response({"error": messages.UNABLE_TO_GENERATE_KEYS}, 500)
            # store public/private key in secret store
            store_secret_key(system_scoped_service, private_key_text, SAIA_PRIVATE_KEY, SAIA_REALM)
            store_secret_key(system_scoped_service, public_jwk, SAIA_PUBLIC_JWK, SAIA_REALM)

        license_key = None
        try:
            license_key = fetch_splunk_license(system_authtoken)
            if not license_key:
                self.logger.error({"error": messages.NO_VALID_LICENSE_FOUND})
        except Exception as e:
            self.logger.error(f"{messages.ERROR_FETCHING_LICENSES}: {str(e)}")

        cc_configuration_collection_object = CloudConnectedConfigurationsCollection(system_scoped_service)

        # store region in cc configs kvstore
        cc_configuration_collection_object.update({CloudConnectedConfigurationsCollection.SAIA_SCS_REGION: region})

        response = json.dumps({
            "public_key": public_jwk,
            "email": email,
            "license": license_key,
            "region": region,
            "company_name": company_name,
            "tenant_name": tenant_name,
            "saia_app_version": saia_app_version,
        })
        encoded_response = ScsUtils.base64url_encode(response)

        self.logger.info(f"encoded_response: {encoded_response}")

        # store encoded response in cc configs kvstore
        cc_configuration_collection_object.update({CloudConnectedConfigurationsCollection.SAIA_ENCODED_ONBOARDING_DATA: encoded_response})

        # when onboarding form is submitted twice (i.e. 1st time persisted in conf file before upgrade & 2nd time persisted in kvstore after upgrade)
        # need to delete the existing conf file entries
        existing_configs_from_conf = ConfUtils.fetch_saia_configs(system_authtoken, SAIA_SCS_STANZA)
        if ConfUtils.content_present_in_any_entry(existing_configs_from_conf):
            ConfUtils.set_scs_configs(system_authtoken, cc_configuration_collection_object.default_entry, SAIA_CONF_FILE, SAIA_SCS_STANZA)

        return {"payload": json.dumps({"value": encoded_response}), "status": 200}
