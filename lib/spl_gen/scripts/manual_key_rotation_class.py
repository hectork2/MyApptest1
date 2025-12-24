import sys, os

splunk_home = os.environ.get("SPLUNK_HOME")
sys.path.insert(0, f'{splunk_home}/etc/apps/Splunk_AI_Assistant_Cloud/lib')


from datetime import timezone, datetime

from splunklib.client import Service
from splunklib.client import connect
from splunklib.binding import handler

from spl_gen.utils import store_secret_key, get_secret_key, delete_secret_key
from spl_gen.scs_utils import ScsUtils
from spl_gen.utils.cloud_connected.conf_utils import ConfUtils

from spl_gen.cloud_connected.cc_configurations.collection import CloudConnectedConfigurationsCollection

from spl_gen.constants import SAIA_REALM, SAIA_SCS_STANZA
from spl_gen.constants import SAIA_PRIVATE_KEY, SAIA_PUBLIC_JWK, SAIA_CONF_FILE

############################################################
# Execute the below steps (in order) for manual key rotation
############################################################

# 1. [CLI Command] - `splunk cmd python3 $SPLUNK_HOME/etc/apps/Splunk_AI_Assistant_Cloud/lib/spl_gen/scripts/manual_key_rotation_class.py fetch_configs {splunk_host_ip} {admin_username} {admin_password}`
# 2. [PO script]   - Execute `saia.cmp-rotate-keys-for-service-principal` PO script. Inputs to the PO script must be provided from the response of the above script
# 3. [CLI Command] - `splunk cmd python3 $SPLUNK_HOME/etc/apps/Splunk_AI_Assistant_Cloud/lib/spl_gen/scripts/manual_key_rotation_class.py test_connection_and_update_configs {splunk_host_ip} {admin_username} {admin_password}`

############################################################
############################################################

SAIA_TEMP_PRIVATE_KEY = 'temp_private_key'
SAIA_TEMP_PUBLIC_JWK = 'temp_public_jwk'

class ManualKeyRotationClass:

    # #############################################################################################################################
    # [IMPORTANT] Both these below methods MUST be executed one after the other. Don't insert methods in between / change the order
    # #############################################################################################################################

    # [Important] MUST be executed before `test_connection_and_update_configs` method is executed
    @classmethod
    def fetch_configs(cls, service: Service):
        scs_access_token = service.token.split(' ')[1]
        configs = ScsUtils.fetch_scs_configs(service, scs_access_token)
        print(f"SAIA configurations: {configs}\n")
        tenant_name = configs[CloudConnectedConfigurationsCollection.SAIA_TENANT_NAME]
        service_principal = configs[CloudConnectedConfigurationsCollection.SAIA_SERVICE_PRINCIPAL]
        scs_region = configs[CloudConnectedConfigurationsCollection.SAIA_SCS_REGION]
        existing_public_jwk = get_secret_key(service, SAIA_PUBLIC_JWK, SAIA_REALM)
        existing_public_jwk_kid = ScsUtils.get_key_id_from_public_key(existing_public_jwk)

        # Delete public/private keys from temporary storage (if exists)
        delete_secret_key(service, SAIA_TEMP_PRIVATE_KEY, SAIA_REALM)
        delete_secret_key(service, SAIA_TEMP_PUBLIC_JWK, SAIA_REALM)

        # Update the newly generated public/private in temporary storage
        new_private_key_text, new_public_jwk, _ = ScsUtils.generate_key_pair()
        store_secret_key(service, new_private_key_text, SAIA_TEMP_PRIVATE_KEY, SAIA_REALM)
        store_secret_key(service, new_public_jwk, SAIA_TEMP_PUBLIC_JWK, SAIA_REALM)

        print(f'tenant_name : {tenant_name}')
        print(f'service_principal: {service_principal}')
        print(f'existing_public_jwk_kid: {existing_public_jwk_kid}')
        print(f'scs_region: {scs_region}')
        print(f'new_public_jwk: {new_public_jwk}')

    # [Important] MUST be executed after `fetch_configs` method is executed
    @classmethod
    def test_connection_and_update_configs(cls, service: Service):
        scs_access_token = service.token.split(' ')[1]

        configs = ScsUtils.fetch_scs_configs(service, scs_access_token)
        print(f"SAIA configurations: {configs}")

        tenant_name = configs[CloudConnectedConfigurationsCollection.SAIA_TENANT_NAME]
        tenant_hostname = configs[CloudConnectedConfigurationsCollection.SAIA_TENANT_HOSTNAME]
        service_principal = configs[CloudConnectedConfigurationsCollection.SAIA_SERVICE_PRINCIPAL]
        scs_region = configs[CloudConnectedConfigurationsCollection.SAIA_SCS_REGION]

        _, scs_environment = ScsUtils.fetch_tenant_and_env_from_tenant_hostname(tenant_hostname)
        current_timestamp = datetime.now(timezone.utc)

        new_private_key_text = get_secret_key(service, SAIA_TEMP_PRIVATE_KEY, SAIA_REALM)
        new_public_jwk = get_secret_key(service, SAIA_TEMP_PUBLIC_JWK, SAIA_REALM)
        new_public_jwk_kid = ScsUtils.get_key_id_from_public_key(new_public_jwk)

        new_scs_token, scs_token_expiry = ScsUtils.generate_scs_token(service_principal, new_private_key_text,
                                                                      new_public_jwk_kid, scs_environment, tenant_name, scs_region)
        ScsUtils.validate_access_token(new_scs_token, tenant_name, tenant_hostname, service)
        ScsUtils.test_saia_search(tenant_hostname, tenant_name, new_scs_token, service)

        # Delete the old public/private keys from permanent storage
        delete_secret_key(service, SAIA_PRIVATE_KEY, SAIA_REALM)
        delete_secret_key(service, SAIA_PUBLIC_JWK, SAIA_REALM)

        # Delete the new public/private keys from temporary storage
        delete_secret_key(service, SAIA_TEMP_PRIVATE_KEY, SAIA_REALM)
        delete_secret_key(service, SAIA_TEMP_PUBLIC_JWK, SAIA_REALM)

        # Update the new public/private in permanent storage
        store_secret_key(service, new_private_key_text, SAIA_PRIVATE_KEY, SAIA_REALM)
        store_secret_key(service, new_public_jwk, SAIA_PUBLIC_JWK, SAIA_REALM)

        CloudConnectedConfigurationsCollection(service).update({
            CloudConnectedConfigurationsCollection.SAIA_SCS_TOKEN: new_scs_token,
            CloudConnectedConfigurationsCollection.SAIA_SCS_TOKEN_EXPIRY: scs_token_expiry,
            CloudConnectedConfigurationsCollection.SAIA_LAST_SETUP_TIMESTAMP: int(current_timestamp.timestamp())
        })

        print(f"Manual rotation of keys is successful, datetime is : {current_timestamp}")

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: splunk cmd python3 $SPLUNK_HOME/etc/apps/Splunk_AI_Assistant_Cloud/lib/spl_gen/scripts/manual_key_rotation_class.py <method_name> <host_ip> <username> <password>")
        sys.exit(1)

    method_name = sys.argv[1]
    host_ip = sys.argv[2]
    user_name = sys.argv[3]
    password = sys.argv[4]

    service = connect(
        app='Splunk_AI_Assistant_Cloud',
        handler=handler(timeout=1),
        host=host_ip,
        username=user_name,
        password=password,
        retries=2
    )

    if method_name == "fetch_configs":
        ManualKeyRotationClass.fetch_configs(service)
    elif method_name == "test_connection_and_update_configs":
        ManualKeyRotationClass.test_connection_and_update_configs(service)
    else:
        print(f"Error: Unknown method '{method_name}'. Choose from: static_method, class_method, instance_method")