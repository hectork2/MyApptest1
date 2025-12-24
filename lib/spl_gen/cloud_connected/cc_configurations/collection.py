from ...collection import SingleEntryCollection

class CloudConnectedConfigurationsCollection(SingleEntryCollection):
    SAIA_SCS_TOKEN = "scs_token"
    SAIA_TENANT_NAME = "tenant_name"
    SAIA_TENANT_HOSTNAME = "tenant_hostname"
    SAIA_SCS_REGION = "scs_region"
    SAIA_SERVICE_PRINCIPAL = "service_principal"
    SAIA_SCS_TOKEN_EXPIRY = "scs_token_expiry"
    SAIA_LAST_SETUP_TIMESTAMP = "last_setup_timestamp"
    SAIA_ENCODED_ONBOARDING_DATA = "encoded_onboarding_data"

    def __init__(self, service):
        super().__init__(service)

    @property
    def collection_name(self):
        return "cloud_connected_configurations"

    @property
    def default_entry(self):
        return {
            self.SAIA_SCS_TOKEN: "",
            self.SAIA_TENANT_NAME: "",
            self.SAIA_TENANT_HOSTNAME: "",
            self.SAIA_SCS_REGION: "",
            self.SAIA_SERVICE_PRINCIPAL: "",
            self.SAIA_SCS_TOKEN_EXPIRY: "",
            self.SAIA_LAST_SETUP_TIMESTAMP: "",
            self.SAIA_ENCODED_ONBOARDING_DATA: "",

        }

    @property
    def keys(self):
        return [self.SAIA_SCS_TOKEN, self.SAIA_TENANT_NAME, self.SAIA_TENANT_HOSTNAME, self.SAIA_SCS_REGION, self.SAIA_SERVICE_PRINCIPAL, self.SAIA_SCS_TOKEN_EXPIRY, self.SAIA_LAST_SETUP_TIMESTAMP, self.SAIA_ENCODED_ONBOARDING_DATA]
