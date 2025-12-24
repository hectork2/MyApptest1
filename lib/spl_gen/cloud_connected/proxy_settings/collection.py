from ...collection import SingleEntryCollection

class ProxySettingsCollection(SingleEntryCollection):
    SAIA_PROXY_ENABLED = "proxy_enabled"
    SAIA_PROXY_TYPE = "proxy_type"
    SAIA_PROXY_HOSTNAME = "proxy_hostname"
    SAIA_PROXY_PORT = "proxy_port"
    SAIA_PROXY_AUTH_ENABLED = "proxy_auth_enabled"
    SAIA_PROXY_USERNAME = "proxy_username"

    def __init__(self, service):
        super().__init__(service)

    @property
    def collection_name(self):
        return "cloud_connected_proxy_settings"

    @property
    def default_entry(self):
        return {
            self.SAIA_PROXY_ENABLED: "",
            self.SAIA_PROXY_TYPE: "",
            self.SAIA_PROXY_HOSTNAME: "",
            self.SAIA_PROXY_PORT: "",
            self.SAIA_PROXY_AUTH_ENABLED: "",
            self.SAIA_PROXY_USERNAME: "",
        }

    @property
    def keys(self):
        return [self.SAIA_PROXY_ENABLED, self.SAIA_PROXY_TYPE, self.SAIA_PROXY_HOSTNAME, self.SAIA_PROXY_PORT, self.SAIA_PROXY_AUTH_ENABLED, self.SAIA_PROXY_USERNAME]
