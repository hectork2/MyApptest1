import os
import json
import hmac
import tempfile
import logging
from typing import Tuple, Union
import subprocess
import hashlib
import base64
import uuid
import http.client
from datetime import timedelta, timezone, datetime
import requests

import splunk
import splunk.rest as rest
from splunklib.client import Service

from spl_gen.utils import store_secret_key, get_secret_key, delete_secret_key, reload_saia_app
from spl_gen.constants import SPLUNK_AI_ASSISTANT_CLOUD_APP, SERVICE_INFO_ENDPOINT
from spl_gen.constants import SAIA_REALM, SAIA_SCS_STANZA, SAIA_KEY_ROTATION_THRESHOLD_IN_DAYS, SAIA_CONF_FILE
from spl_gen.constants import SAIA_PRIVATE_KEY, SAIA_PUBLIC_JWK, SCS_DOMAIN
from spl_gen.errors import messages
from spl_gen.utils import log_kwargs

from spl_gen.utils.cloud_connected.conf_utils import ConfUtils
from spl_gen.utils.cloud_connected.proxy_settings_utils import ProxySettingsUtils
from spl_gen.utils.cloud_connected.shcluster_utils import SHClusterUtils

from spl_gen.cloud_connected.cc_configurations.collection import CloudConnectedConfigurationsCollection

openssl_path = f'{os.environ["SPLUNK_HOME"]}/bin/openssl'


class ScsUtils:

    @classmethod
    def set_logger(cls, logger: logging.Logger):
        cls.logger = logger
        ProxySettingsUtils.set_logger(cls.logger)
        ConfUtils.set_logger(cls.logger)
        SHClusterUtils.set_logger(cls.logger)

    @classmethod
    def base64url_encode(cls, data: Union[str, bytes]) -> str:
        """
        Encodes the input data into a URL-safe base64 format. The returned string is guaranteed to be URL-safe and can
            be safely used in contexts like JWT tokens

        The method removes trailing '=' padding from the encoded string.
        This is done because:
        1. The '=' character is not URL-safe.
        2. The padding is not necessary for decoding in most base64 implementations.
        3. It reduces the overall length of the encoded string.

        The final UTF-8 decoding step is performed because:
        - base64.urlsafe_b64encode() returns bytes, not a string.

        Args:
            data (str or bytes): The data to encode.

        Returns:
            str: The base64url-encoded string.
        """
        if isinstance(data, str):
            data = data.encode('utf-8')
        return base64.urlsafe_b64encode(data).rstrip(b'=').decode('utf-8')

    @classmethod
    def base64url_decode(cls, data: str) -> str:
        """
        Decodes a URL-safe base64 encoded string back to bytes.

        The method adds padding back to the encoded string before decoding.
        This is necessary because base64 encoding requires padding to ensure the length of the encoded string is a multiple of 4.

        Args:
            data (str): The base64url-encoded string.

        Returns:
            bytes: The decoded bytes.
        """
        padding_needed = 4 - (len(data) % 4)
        if padding_needed < 4:
            data += '=' * padding_needed
        return base64.urlsafe_b64decode(data).decode('utf-8')

    @classmethod
    def get_passphrase(cls, principal_id: str) -> str:
        """
        Generates a passphrase for the private key based on the principal ID.

        Args:
            principal_id (str): The identifier of the principal.

        Returns:
            str: A hexadecimal passphrase derived from the principal id.
        """
        salt = hashlib.sha256(principal_id.encode()).hexdigest()
        h = hmac.new(salt.encode(), principal_id.encode(), hashlib.sha256).hexdigest()
        return h

    @classmethod
    def _extract_ecdsa_signature_components(cls, signature: bytes) -> Tuple[bytes, bytes]:
        """
        Extract the r and s components from an ECDSA signature.

        In ECDSA, a signature consists of two values, r and s:
        - r: The x-coordinate of a random point on the elliptic curve.
        - s: Calculated using the private key, the hash of the message, and r.

        Args:
            signature (bytes): The ECDSA signature in DER format.

        Returns:
            tuple: A tuple containing the r and s values as bytes, each padded to 32 bytes.
        """
        parsed_asn1_result = subprocess.run(
            [openssl_path, 'asn1parse', '-inform', 'DER', '-i'],
            input=signature,
            capture_output=True,
            check=True,
        )
        if not parsed_asn1_result or not parsed_asn1_result.stdout:
            error_msg = 'null asn1 signature' if not parsed_asn1_result else 'empty asn1 signature'
            raise Exception(f'Error parsing signature: {error_msg}')

        output = parsed_asn1_result.stdout.decode('utf-8')
        integer_values = [line.split(':')[-1].strip() for line in output.split('\n') if 'INTEGER' in line]
        if len(integer_values) != 2:
            raise Exception('Failed to extract R and S values from signature')

        r_value = cls._pad_to_32_bytes(bytes.fromhex(integer_values[0]))
        s_value = cls._pad_to_32_bytes(bytes.fromhex(integer_values[1]))

        return r_value, s_value

    @classmethod
    def _pad_to_32_bytes(cls, data: bytes) -> bytes:
        """
        Pad the input data to 32 bytes by adding leading zero bytes.

        Args:
            data (bytes): The input data to pad.

        Returns:
            bytes: The padded data.
        """
        return data.rjust(32, b'\x00')

    @classmethod
    def _sign_ecdsa(cls, data: str, private_key: str, passphrase: str) -> str:
        """
        Sign data using an ECDSA private key.

        Args:
            data (str): The data to sign.
            private_key (str): The ECDSA private key in PEM format.
            passphrase(str): The passphrase to decrypt the private key.

        Returns:
            str: The base64url-encoded signature.
        """
        # Create a temporary file with delete=False to ensure it doesn't get deleted immediately - for Windows compatibility
        with tempfile.NamedTemporaryFile('w', delete=False) as key_file:
            key_file.write(private_key)
            key_file.flush()
            temp_key_file_path = key_file.name  # Save the path for later use
        try:

            signing_result = subprocess.run(
                [openssl_path, 'dgst', '-sha256', '-sign', temp_key_file_path, '-passin', f'pass:{passphrase}'],
                input=data.encode('utf-8'),
                capture_output=True,
                check=True,
            )
            if not signing_result or not signing_result.stdout:
                error_msg = 'null signing result' if not signing_result else 'empty signature'
                raise Exception(f'Failed to sign data: {error_msg}')

            signature = signing_result.stdout
            r_value, s_value = cls._extract_ecdsa_signature_components(signature)
            combined_signature = r_value + s_value
            return cls.base64url_encode(combined_signature)
        finally:
            # Cleanup: Delete the temporary key file after use
            if os.path.exists(temp_key_file_path):
                os.remove(temp_key_file_path)

    @classmethod
    def generate_scs_auth_url(cls, scs_environment, scs_region):
        scs_auth_url = f"https://region-{scs_region}.auth.{scs_environment}{SCS_DOMAIN}/token"
        return scs_auth_url

    @classmethod
    def generate_scs_token(cls, service_principal, private_key_text, public_jwk_key_id, scs_environment, tenant_name, scs_region, service: Service):
        scs_auth_url = cls.generate_scs_auth_url(scs_environment, scs_region)

        # Parse Private KEY
        jwt_headers = {
            "alg": "ES256",
            "typ": "JWT",
            "kid": public_jwk_key_id
        }
        time_now = datetime.now(timezone.utc)
        expiry_time = int((time_now + timedelta(hours=1)).timestamp())
        request_id = str(uuid.uuid4())
        jwt_payload = {
            'iss': service_principal,
            'sub': service_principal,
            'iat': int(time_now.timestamp()),
            'exp': expiry_time,  # This cannot be more than 1 hour apart from "iat"
            "jti": request_id,  # This is a random generated by client
            "aud": [scs_auth_url],
            'tenant': tenant_name
        }

        encoded_headers = cls.base64url_encode(json.dumps(jwt_headers, separators=(',', ':')))
        encoded_payload = cls.base64url_encode(json.dumps(jwt_payload, separators=(',', ':')))

        passphrase = cls.get_passphrase(principal_id=service_principal)
        unsigned_token = f'{encoded_headers}.{encoded_payload}'
        signature = cls._sign_ecdsa(unsigned_token, private_key_text, passphrase)

        # Combine the unsigned token and the signature, separated by a dot
        jwt_token = f"{unsigned_token}.{signature}"

        request_data = {
            "grant_type": "client_credentials",
            "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": jwt_token
        }
        cls.logger.info(
            log_kwargs(
                message="Sending Auth request to IAC",
                tenant_id=tenant_name,
                request_id=request_id
            )
        )

        try:
            resp = requests.post(scs_auth_url, data=request_data, timeout=15, proxies=ProxySettingsUtils.fetch_proxies_if_enabled(service))
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            error = f"IAC Authentication failed with HTTP code {e.response.status_code}"
            cls.logger.error(
                log_kwargs(
                    message=error,
                    tenant_id=tenant_name,
                    request_id=request_id
                )
            )
            raise Exception(messages.ERROR_GENERATING_SCS_ACCESS_TOKEN + '. ' + messages.CONTACT_SPLUNK_SUPPORT)

        resp_json = resp.json()
        auth_token = resp_json['access_token']
        return auth_token, expiry_time

    @classmethod
    def validate_access_token(cls, auth_token, tenant_name, tenant_hostname, service: Service):
        request_id = str(uuid.uuid4())
        cls.logger.info(
            log_kwargs(
                message="Validating SCS Access token",
                tenant_id=tenant_name,
                request_id=request_id
            )
        )
        api_version = "v3"
        headers = {
            "Authorization": "Bearer " + auth_token,
            "Content-type": "application/json",
            "X-Request-ID": request_id   # Unpack the tuple to get the actual request_id
        }
        try:
            res = requests.get(
                    f"https://{tenant_hostname}/{tenant_name}/identity/{api_version}/validate?include=principal,tenant",
                    headers=headers, timeout=15, proxies=ProxySettingsUtils.fetch_proxies_if_enabled(service))
            res.raise_for_status()
        except requests.exceptions.HTTPError as e:
            cls.logger.error(
                log_kwargs(
                    message=f'SCS Access token validation failed with status code: {e.response.status_code}',
                    tenant_id=tenant_name,
                    request_id=request_id
                )
            )
            raise Exception(messages.ERROR_VALIDATING_SCS_ACCESS_TOKEN + '. ' + messages.CONTACT_SPLUNK_SUPPORT)

        cls.logger.info(
            log_kwargs(
                message="SCS Access token is validated",
                tenant_id=tenant_name,
                request_id=request_id
            )
        )
        return

    @classmethod
    def test_saia_search(cls, tenant_hostname, tenant_name, api_key, service: Service):
        api_version = "v1alpha1"
        path = "api/metadata"

        if tenant_hostname and not tenant_hostname.startswith("http"):
            tenant_hostname = f"https://{tenant_hostname}"
        url = f"{tenant_hostname}/{tenant_name}/saia-api/{api_version}/{path}"
        request_id = str(uuid.uuid4())
        res = requests.request("GET", url, headers={"authorization": f"Bearer {api_key}", "X-Request-ID": request_id}, proxies=ProxySettingsUtils.fetch_proxies_if_enabled(service), timeout=60)
        res.raise_for_status()
        cls.logger.info(
            log_kwargs(
                message="Test saia search is successful",
                tenant_id=tenant_name,
                request_id=request_id
            )
        )

    @classmethod
    def generate_private_key(cls):
        # Generate the EC private key using OpenSSL
        result = subprocess.run(
            [openssl_path, "ecparam", "-genkey", "-name", "secp256r1", "-noout"],
            capture_output=True, text=True
        )
        private_key = result.stdout
        return private_key

    @classmethod
    def extract_public_key(cls, private_key):
        # Use the private key to extract the public key
        result = subprocess.run(
            [openssl_path, "ec", "-pubout"],
            input=private_key, capture_output=True, text=True
        )
        public_key = result.stdout
        return public_key

    @classmethod
    def extract_xy_coordinates(cls, public_key):
        # Convert public key to DER format and extract x, y coordinates
        result = subprocess.run(
            [openssl_path, "ec", "-pubin", "-pubout", "-outform", "DER"],
            input=public_key.encode(), capture_output=True
        )
        der = result.stdout
        # The last 65 bytes are the uncompressed EC public key
        x_bytes = der[-64:-32]
        y_bytes = der[-32:]
        x = base64.urlsafe_b64encode(x_bytes).rstrip(b'=').decode('utf-8')
        y = base64.urlsafe_b64encode(y_bytes).rstrip(b'=').decode('utf-8')
        return x, y

    @classmethod
    def generate_kid(cls, x, y):
        # Example: Create a SHA-256 hash of x and y concatenated
        kid_input = (x + y).encode('utf-8')
        kid = hashlib.sha256(kid_input).digest()
        kid_base64 = base64.urlsafe_b64encode(kid).rstrip(b'=').decode('utf-8')
        return kid_base64

    @classmethod
    def create_jwk(cls, x, y):
        jwk = {
            "kty": "EC",
            "crv": "P-256",
            "x": x,
            "y": y,
            "kid": cls.generate_kid(x, y)
        }
        return jwk

    @classmethod
    def generate_key_pair(cls):
        private_key = cls.generate_private_key()
        public_key = cls.extract_public_key(private_key)

        x,y = cls.extract_xy_coordinates(public_key)
        jwk = cls.create_jwk(x, y)

        public_jwk_json = json.dumps(jwk, indent=2)

        return private_key, public_jwk_json, jwk['kid']

    @classmethod
    def fetch_tenant_hostname_from_payload(cls, payload_tenant_hostname):
        tenant_hostname = 'cmp-' + payload_tenant_hostname if not payload_tenant_hostname.startswith('cmp-') else payload_tenant_hostname
        return tenant_hostname

    @classmethod
    def fetch_tenant_and_env_from_tenant_hostname(cls, tenant_hostname):
        tenant_name, _, last = tenant_hostname.partition('.api.')
        scs_env, _, _ = last.partition(SCS_DOMAIN)
        return tenant_name, scs_env

    @classmethod
    def get_server_info(cls, session_key: str):
        cls.logger.debug("Getting the stack information")
        try:
            response, content = rest.simpleRequest(
                SERVICE_INFO_ENDPOINT,
                sessionKey=session_key,
                getargs={"output_mode": "json"},
                raiseAllErrors=True,
            )
            cls.logger.debug(f"Got status {response.status} when fetching Splunk server info")
            return json.loads(content)

        except splunk.ResourceNotFound:
            cls.logger.exception(f"{SERVICE_INFO_ENDPOINT} endpoint does not exist on this stack")
        except Exception as e:
            cls.logger.exception(f"Failed to get stack information {str(e)}")
            raise Exception("Failed to get stack information")
        return None

    @classmethod
    def is_cloud_stack(cls, session_key):
        """Returns if the current stack is a cloud stack or not.

        Args:
            session_key: Splunkd session key.

        Returns:
            boolean
        """
        payload = cls.get_server_info(session_key)
        if payload:
            entry = payload.get("entry")
            if entry:
                content = entry[0].get("content")
                if content:
                    instance_type = content.get("instance_type")
                    if instance_type == 'cloud':
                        cls.logger.debug(f"The Splunk instance type is {instance_type}")
                        return True
        cls.logger.debug("The Splunk instance type is not cloud")
        return False

    @classmethod
    def fetch_and_migrate_to_kv_store_if_needed(cls, service, session_key, cc_configuration_collection_object):
        cc_configurations = cc_configuration_collection_object.get()
        configs_from_kvstore = {**cc_configurations}

        if not configs_from_kvstore or not configs_from_kvstore[CloudConnectedConfigurationsCollection.SAIA_SCS_REGION]:
            request_id = str(uuid.uuid4())
            cls.logger.info(f"Migrating cloud_connected_configurations from conf file to kvstore, request_id: {request_id}")
            configs_from_conf = ConfUtils.fetch_saia_configs(session_key, SAIA_SCS_STANZA)
            cls.logger.info(f"cloud_connected_configurations from conf file while migrating : {configs_from_conf}, request_id: {request_id}")
            cc_configuration_collection_object.update(configs_from_conf)
            cc_configurations = cc_configuration_collection_object.get()
            configs_from_kvstore = {**cc_configurations}

            ConfUtils.set_scs_configs(service.token, cc_configuration_collection_object.default_entry, SAIA_CONF_FILE, SAIA_SCS_STANZA)
            cls.logger.info(f"Successfully migrated cloud_connected_configurations from conf file to kvstore, request_id: {request_id}")

        return configs_from_kvstore

    @classmethod
    def fetch_scs_region(cls, service, session_key):
        cc_configuration_collection_object = CloudConnectedConfigurationsCollection(service)
        configs = ScsUtils.fetch_and_migrate_to_kv_store_if_needed(service, session_key, cc_configuration_collection_object)
        return configs[CloudConnectedConfigurationsCollection.SAIA_SCS_REGION]

    @classmethod
    def fetch_scs_configs(cls, service, session_key):
        cc_configuration_collection_object = CloudConnectedConfigurationsCollection(service)
        configs = ScsUtils.fetch_and_migrate_to_kv_store_if_needed(service, session_key, cc_configuration_collection_object)

        if not configs or not all(key in configs and configs.get(key) for key in cc_configuration_collection_object.keys):
            error = "SCS configs are not available properly for on-prem stack"
            cls.logger.error(error)
            raise Exception(error)

        filtered_scs_configs = {key: configs[key] for key in cc_configuration_collection_object.keys}
        return filtered_scs_configs

    @classmethod
    def get_scs_token_for_cmp_stack(cls, service: Service):
        configs = ScsUtils.fetch_scs_configs(service, service.token)
        scs_token = configs[CloudConnectedConfigurationsCollection.SAIA_SCS_TOKEN]
        scs_token_expiry = configs[CloudConnectedConfigurationsCollection.SAIA_SCS_TOKEN_EXPIRY]

        # if [current_timestamp + 5 seconds] (future time) surpasses the expiry time -> then rotate the scs_token
        if int((datetime.now(timezone.utc) + timedelta(seconds=5)).timestamp()) > int(scs_token_expiry):
            scs_token, scs_token_expiry = ScsUtils.refresh_scs_token_for_cmp_stack(configs, service)
        return scs_token

    @classmethod
    def get_key_id_from_public_key(cls, public_jwk):
        public_jwk_replaced = json.loads(public_jwk.replace("'", '"'))
        return public_jwk_replaced['kid']

    @classmethod
    def refresh_scs_token_for_cmp_stack(cls, configs, service: Service):
        tenant_name = configs[CloudConnectedConfigurationsCollection.SAIA_TENANT_NAME]
        cls.logger.debug(
            log_kwargs(
                message="Refreshing scs access token",
                tenant_id=tenant_name
            )
        )
        private_key_text = get_secret_key(service, SAIA_PRIVATE_KEY, SAIA_REALM)
        public_jwk = get_secret_key(service, SAIA_PUBLIC_JWK, SAIA_REALM)
        public_jwk_kid = ScsUtils.get_key_id_from_public_key(public_jwk)
        _, scs_environment = ScsUtils.fetch_tenant_and_env_from_tenant_hostname(configs[CloudConnectedConfigurationsCollection.SAIA_TENANT_HOSTNAME])
        scs_token, scs_token_expiry = ScsUtils.generate_scs_token(configs[CloudConnectedConfigurationsCollection.SAIA_SERVICE_PRINCIPAL], private_key_text, public_jwk_kid, scs_environment, tenant_name, configs[CloudConnectedConfigurationsCollection.SAIA_SCS_REGION], service)

        CloudConnectedConfigurationsCollection(service).update({
            CloudConnectedConfigurationsCollection.SAIA_SCS_TOKEN: scs_token,
            CloudConnectedConfigurationsCollection.SAIA_SCS_TOKEN_EXPIRY: scs_token_expiry
        })

        cls.logger.info(
            log_kwargs(
                message="Refreshed scs token",
                tenant_id=tenant_name
            )
        )
        return scs_token, scs_token_expiry

    @classmethod
    def update_app_configured(cls, service: Service, system_authtoken, session_authtoken, value: bool):
        cls.logger.info(f"Updating is_configured in app.conf for this instance")
        ConfUtils.update_is_configured_in_app_conf(system_authtoken, value)

        # Search Head Cluster setup
        shc_members = SHClusterUtils.fetch_shc_members(system_authtoken)
        if shc_members:
            cls.logger.info(f"Updating is_configured in app.conf for SHC members: {shc_members}")
            SHClusterUtils.update_is_configured_in_app_conf(session_authtoken, shc_members, value)


    @classmethod
    def add_public_key_to_service_principal(cls, service: Service, service_principal, public_jwk, scs_environment, scs_region):
        url = f"https://region-{scs_region}.api.{scs_environment}{SCS_DOMAIN}/system/identity/v3/principals/{service_principal}/keys"
        scs_token = ScsUtils.get_scs_token_for_cmp_stack(service)
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + scs_token
        }

        response = requests.post(url, headers=headers, data=public_jwk, timeout=10, proxies=ProxySettingsUtils.fetch_proxies_if_enabled(service))
        cls.logger.info(f"add_public_key_to_service_principal response : {response.text}")
        if response.status_code != 200 or response.json()['status'] != 'active':
            cls.logger.error(f"Failed adding public key. Status code: {response.status_code}, response: {response.text}")
            error = 'Unable to update new public key to service_principal'
            raise Exception(error)

        cls.logger.info(f"add_public_key_to_service_principal : success")

    @classmethod
    def delete_public_key_from_service_principal(cls, service: Service, service_principal, key_id, scs_environment, scs_region):
        url = f"https://region-{scs_region}.api.{scs_environment}{SCS_DOMAIN}/system/identity/v3/principals/{service_principal}/keys/{key_id}"
        scs_token = ScsUtils.get_scs_token_for_cmp_stack(service)

        response = requests.delete(url, headers={ "Authorization": "Bearer " + scs_token }, timeout=10, proxies=ProxySettingsUtils.fetch_proxies_if_enabled(service))
        cls.logger.info(f"delete_public_key_from_service_principal response : {response.text}")
        if response.status_code != 204:
            cls.logger.error(f"Failed deleting public key. Status code: {response.status_code}, response: {response.text}")
            error = 'Unable to delete existing public key from service_principal'
            raise Exception(error)

        cls.logger.info(f"delete_public_key_from_service_principal : success")

    @classmethod
    def rotate_public_private_keys(cls, service: Service):
        cls.logger.info(f"Rotation of private/public keys")
        configs = ScsUtils.fetch_scs_configs(service, service.token)
        cls.logger.info(f"SAIA configurations: {configs}")

        last_setup_timestamp = configs[CloudConnectedConfigurationsCollection.SAIA_LAST_SETUP_TIMESTAMP]
        tenant_name = configs[CloudConnectedConfigurationsCollection.SAIA_TENANT_NAME]
        tenant_hostname = configs[CloudConnectedConfigurationsCollection.SAIA_TENANT_HOSTNAME]
        service_principal = configs[CloudConnectedConfigurationsCollection.SAIA_SERVICE_PRINCIPAL]
        scs_region = configs[CloudConnectedConfigurationsCollection.SAIA_SCS_REGION]

        _, scs_environment = ScsUtils.fetch_tenant_and_env_from_tenant_hostname(tenant_hostname)
        current_timestamp = datetime.now(timezone.utc)

        # [current_timestamp - 85 days] surpasses the last_setup_timestamp -> then rotate the keys
        if int((current_timestamp - timedelta(days=SAIA_KEY_ROTATION_THRESHOLD_IN_DAYS)).timestamp()) < int(last_setup_timestamp):
            cls.logger.info(f"{SAIA_KEY_ROTATION_THRESHOLD_IN_DAYS} days must pass after previous setup for key rotation. Exiting ...")
        else:
            cls.logger.info(f"Executing rotation of public & private keys")
            cls.logger.info(f"SAIA configurations : {configs} & current time is {current_timestamp}")

            try:
                private_key_text, public_jwk, public_jwk_kid = ScsUtils.generate_key_pair()
                ScsUtils.add_public_key_to_service_principal(service, service_principal, public_jwk, scs_environment, scs_region)

                new_scs_token, scs_token_expiry = ScsUtils.generate_scs_token(service_principal, private_key_text, public_jwk_kid, scs_environment, tenant_name, scs_region, service)
                ScsUtils.validate_access_token(new_scs_token, tenant_name, tenant_hostname, service)
                ScsUtils.test_saia_search(tenant_hostname, tenant_name, new_scs_token, service)

                old_public_jwk = get_secret_key(service, SAIA_PUBLIC_JWK, SAIA_REALM)
                old_public_jwk_kid = ScsUtils.get_key_id_from_public_key(old_public_jwk)
                ScsUtils.delete_public_key_from_service_principal(service, service_principal, old_public_jwk_kid, scs_environment, scs_region)

                store_secret_key(service, private_key_text, SAIA_PRIVATE_KEY, SAIA_REALM, delete_before_store=True)
                store_secret_key(service, public_jwk, SAIA_PUBLIC_JWK, SAIA_REALM, delete_before_store=True)

                CloudConnectedConfigurationsCollection(service).update({
                    CloudConnectedConfigurationsCollection.SAIA_SCS_TOKEN: new_scs_token,
                    CloudConnectedConfigurationsCollection.SAIA_SCS_TOKEN_EXPIRY: scs_token_expiry,
                    CloudConnectedConfigurationsCollection.SAIA_LAST_SETUP_TIMESTAMP: int(current_timestamp.timestamp())
                })

                cls.logger.info(f"Rotation of keys is successful, datetime is : {current_timestamp}")

            except Exception as e:
                error = f"Unable to rotate private/public key. Exception: {str(e)}"
                cls.logger.error(error)
                raise Exception(error)

    @classmethod
    def cron_for_key_rotation(cls, action, service: Service):
        script_name = 'maintenance_key_rotation.py'
        endpoint = f"/servicesNS/nobody/{SPLUNK_AI_ASSISTANT_CLOUD_APP}/data/inputs/script/%24SPLUNK_HOME%252Fetc%252Fapps%252F{SPLUNK_AI_ASSISTANT_CLOUD_APP}%252Fbin%252F{script_name}/{action}"

        r, c = rest.simpleRequest(
            method='POST',
            path=endpoint,
            sessionKey=service.token,
            getargs={'output_mode': 'json'}
        )
        if r and r.status == http.client.OK:
            cls.logger.info(f"Successfully {action}d cron for key rotation")
            return

        status_code = r.status if r else None
        cls.logger.error(f'Failed to {action} cron for key rotation. Status code {status_code} & response : {vars(r)}')
        if action == 'enable':
            raise Exception(messages.FAILED_ENABLING_CRON_FOR_KEY_ROTATION)
        else:
            raise Exception(messages.FAILED_DISABLING_FOR_KEY_ROTATION)

    @classmethod
    def is_non_captain_in_shcluster_setup(cls, session_key: str) -> bool:
        non_captain_role_name = 'shc_member'
        payload = cls.get_server_info(session_key)
        if payload:
            content = payload.get("entry")[0].get("content")
            if content and non_captain_role_name in content.get("server_roles", []):
                return True
        return False