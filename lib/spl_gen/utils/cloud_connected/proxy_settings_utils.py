import logging
import uuid

from splunklib.client import Service

from spl_gen.utils import store_secret_key, get_secret_key, delete_secret_key, reload_saia_app
from spl_gen.utils.cloud_connected.conf_utils import ConfUtils
from spl_gen.cloud_connected.proxy_settings.collection import ProxySettingsCollection

from spl_gen.constants import SAIA_SCS_STANZA, SAIA_REALM, SAIA_CONF_FILE
from spl_gen.constants import SAIA_PROXY_SETTINGS_STANZA, SAIA_PROXY_PASSWORD

class ProxySettingsUtils:

    @classmethod
    def set_logger(cls, logger: logging.Logger):
        cls.logger = logger
        ConfUtils.set_logger(cls.logger)

    @classmethod
    def is_proxy_settings_enabled(cls, service):
        proxy_settings = ProxySettingsUtils.fetch_proxy_configs_and_migrate_to_kv_store_if_needed(service)
        return False if proxy_settings is None else True

    @classmethod
    def is_valid_proxy_settings_and_auth_present(cls, proxy_settings):
        if proxy_settings.get('type') not in ("http", "https"):
            cls.logger.error('Invalid scheme')
            return False, False
        if not proxy_settings.get('hostname') or not proxy_settings.get('port'):
            cls.logger.error('Missing host or port')
            return False, False
        if proxy_settings.get('username') or proxy_settings.get('password'):  # Proxy auth enabled. Replicating AWS TA App's UX
            return True, True
        return True, False

    @classmethod
    def store_proxy_settings(cls, service: Service, proxy_settings_from_request, proxy_auth_present):
        proxy_configs = {
            ProxySettingsCollection.SAIA_PROXY_ENABLED: 'true',
            ProxySettingsCollection.SAIA_PROXY_TYPE: proxy_settings_from_request['type'],
            ProxySettingsCollection.SAIA_PROXY_HOSTNAME: proxy_settings_from_request['hostname'],
            ProxySettingsCollection.SAIA_PROXY_PORT: proxy_settings_from_request['port'],
        }

        if proxy_auth_present:
            proxy_configs[ProxySettingsCollection.SAIA_PROXY_AUTH_ENABLED] = 'true'
            proxy_configs[ProxySettingsCollection.SAIA_PROXY_USERNAME] = proxy_settings_from_request['username']
            store_secret_key(service, proxy_settings_from_request['password'], SAIA_PROXY_PASSWORD, SAIA_REALM,
                             delete_before_store=True)
        else:
            proxy_configs[ProxySettingsCollection.SAIA_PROXY_AUTH_ENABLED] = 'false'
            proxy_configs[ProxySettingsCollection.SAIA_PROXY_USERNAME] = ''
            delete_secret_key(service, SAIA_PROXY_PASSWORD, SAIA_REALM)

        ProxySettingsCollection(service).update(proxy_configs)

    @classmethod
    def construct_proxy_url(cls, service, proxy_configs):
        constructed_proxy_url = proxy_configs[ProxySettingsCollection.SAIA_PROXY_TYPE] + '://'

        if proxy_configs[ProxySettingsCollection.SAIA_PROXY_AUTH_ENABLED] == 'true':
            password = get_secret_key(service, SAIA_PROXY_PASSWORD, SAIA_REALM)
            username_and_password = proxy_configs[ProxySettingsCollection.SAIA_PROXY_USERNAME] + ':' + password
            constructed_proxy_url += username_and_password + '@'

        host_and_port = proxy_configs[ProxySettingsCollection.SAIA_PROXY_HOSTNAME] + ':' + proxy_configs[ProxySettingsCollection.SAIA_PROXY_PORT]
        return constructed_proxy_url + host_and_port

    @classmethod
    def fetch_proxies_if_enabled(cls, service):
        proxy_configs = ProxySettingsUtils.fetch_proxy_configs_and_migrate_to_kv_store_if_needed(service)
        if not proxy_configs:
            return {}

        constructed_proxy_url = ProxySettingsUtils.construct_proxy_url(service, proxy_configs)
        return {
            'http': constructed_proxy_url,
            'https': constructed_proxy_url,
        }

    @classmethod
    def migrate_conf_to_kvstore(cls, service, proxy_settings_collection_object):
        request_id = str(uuid.uuid4())
        proxy_configs_from_kvstore = {}
        proxy_configs_from_conf_file = ConfUtils.fetch_saia_configs(service.token, SAIA_PROXY_SETTINGS_STANZA)
        cls.logger.info(f"cloud_connected_proxy_settings from conf file : {proxy_configs_from_conf_file}, request_id: {request_id}")
        cls.logger.info(f"Migrating cloud_connected_proxy_settings from conf file to kvstore, request_id: {request_id}")

        try:
            if not ConfUtils.content_present_in_any_entry(proxy_configs_from_conf_file):
                # This makes 'proxy_enabled' as false in kvstore thereby not needing to migrate to kvstore again
                ProxySettingsUtils.clear_proxy_settings(service, False)
            else:
                # migrate to kvstore & save empty content in conf file
                proxy_settings_collection_object.update(proxy_configs_from_conf_file)
                proxy_settings = proxy_settings_collection_object.get()

                ConfUtils.set_scs_configs(service.token, proxy_settings_collection_object.default_entry, SAIA_CONF_FILE,
                                          SAIA_PROXY_SETTINGS_STANZA)
                proxy_configs_from_kvstore = {**proxy_settings}

            cls.logger.info(f"Successfully migrated cloud_connected_proxy_settings from conf file to kvstore, request_id: {request_id}")
        except Exception as e:
            cls.logger.error(f"Error migrating cloud_connected_proxy_settings from conf file to kvstore, request_id: {request_id}, error: {str(e)}")

        return proxy_configs_from_kvstore

    @classmethod
    def fetch_proxy_configs_and_migrate_to_kv_store_if_needed(cls, service):
        proxy_settings_collection_object = ProxySettingsCollection(service)
        proxy_settings = proxy_settings_collection_object.get()
        proxy_configs_from_kvstore = {**proxy_settings}

        if proxy_configs_from_kvstore and proxy_configs_from_kvstore[ProxySettingsCollection.SAIA_PROXY_ENABLED]:
            if proxy_configs_from_kvstore[ProxySettingsCollection.SAIA_PROXY_ENABLED] == 'false': return {}
            else: return proxy_configs_from_kvstore
        else:
            return ProxySettingsUtils.migrate_conf_to_kvstore(service, proxy_settings_collection_object)

    @classmethod
    def fetch_all_proxy_settings(cls, service, fetch_password = False):
        proxy_configs = ProxySettingsUtils.fetch_proxy_configs_and_migrate_to_kv_store_if_needed(service)
        if not proxy_configs:
            return {}

        proxy_settings = {
            'type': proxy_configs[ProxySettingsCollection.SAIA_PROXY_TYPE],
            'hostname': proxy_configs[ProxySettingsCollection.SAIA_PROXY_HOSTNAME],
            'port': proxy_configs[ProxySettingsCollection.SAIA_PROXY_PORT],
        }

        if proxy_configs[ProxySettingsCollection.SAIA_PROXY_AUTH_ENABLED] == 'true':
            proxy_settings['username'] = proxy_configs[ProxySettingsCollection.SAIA_PROXY_USERNAME]
            if fetch_password: proxy_settings['password'] = get_secret_key(service, SAIA_PROXY_PASSWORD, SAIA_REALM)

        return proxy_settings


    @classmethod
    def clear_proxy_settings(cls, service: Service, delete_proxy_password = True):
        proxy_settings_collection_object = ProxySettingsCollection(service)
        proxy_configs = proxy_settings_collection_object.default_entry
        proxy_configs[ProxySettingsCollection.SAIA_PROXY_ENABLED] = 'false'
        proxy_settings_collection_object.update(proxy_configs)
        if delete_proxy_password: delete_secret_key(service, SAIA_PROXY_PASSWORD, SAIA_REALM)