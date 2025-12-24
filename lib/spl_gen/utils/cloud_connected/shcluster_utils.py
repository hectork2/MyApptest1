import json
import logging
import os

import requests

import splunk
import splunk.rest as rest
from splunklib.client import Service

from spl_gen.constants import SPLUNK_AI_ASSISTANT_CLOUD_APP
from spl_gen.errors import messages

class SHClusterUtils:

    @classmethod
    def set_logger(cls, logger: logging.Logger):
        cls.logger = logger

    @classmethod
    def fetch_root_ca_path(cls):
        from splunk.clilib.info_gather import get_server_conf, SSL_CONFIG_STANZA
        server_conf = get_server_conf()
        sslconfig_stanza = server_conf.get(SSL_CONFIG_STANZA, {})
        if 'sslRootCAPath' not in sslconfig_stanza:
            sslconfig_stanza['sslRootCAPath'] = sslconfig_stanza.get('caCertFile')
        ssl_root_ca_path = sslconfig_stanza.get('sslRootCAPath', None)
        if ssl_root_ca_path is not None:
            ssl_root_ca_path = os.path.expandvars(ssl_root_ca_path)
        return ssl_root_ca_path

    @classmethod
    def fetch_certs(cls):
        try:
            from splunk.rest import getWebCertFile, getWebKeyFile
            local_web_key = getWebKeyFile()
            local_web_cert = getWebCertFile()
        except ImportError:
            local_web_key = None
            local_web_cert = None
        if local_web_cert and local_web_key:
            return (local_web_cert, local_web_key)
        return None

    @classmethod
    def fetch_shc_members(cls, system_authtoken):
        shc_members = []
        try:
            response, content = rest.simpleRequest(
                '/services/shcluster/status',
                sessionKey=system_authtoken,
                getargs={"output_mode": "json"},
            )
            cls.logger.info(f"Got status {response.status} when fetching SHC members with content: {content}")
            if response.status != 200:
                cls.logger.info('SHC status API returned non-200 status code, assuming not a SH Cluster')
                return shc_members

            payload = json.loads(content)
            entries = payload.get("entry", [])
            for entry in entries:
                content = entry.get("content", {})
                peers = content.get("peers", {})
                for peer_id, peer_info in peers.items():
                    shc_members.append({
                        'mgmt_uri': peer_info.get("mgmt_uri"),
                        'mgmt_uri_alias': peer_info.get("mgmt_uri_alias")
                    })
        except Exception as e:
            cls.logger.exception(f"Failed to get SHC members information {str(e)}")
            raise Exception("Failed to get SHC members information")
        return shc_members

    @classmethod
    def update_app_configured_local_handler(cls, uri, headers, value, verify=False, cert=None) -> bool:
        cls.logger.info(f"Updating is_configured in app.conf on SHC member using uri {uri} with verify={verify}, cert={cert}")
        url = f"{uri}/servicesNS/nobody/{SPLUNK_AI_ASSISTANT_CLOUD_APP}/apps/local/{SPLUNK_AI_ASSISTANT_CLOUD_APP}?output_mode=json"
        try:
            response = requests.post(url, headers=headers, data={'configured': value}, verify=verify, cert=cert, timeout=5)
            if response.status_code == 200:
                cls.logger.info(f"Successfully updated is_configured in app.conf on SHC member with uri {uri}")
                return True
            else:
                cls.logger.error(f"Failed to update is_configured in app.conf on SHC member with uri {uri}. Status code: {response.status_code} & response: {response.text}")
                return False
        except Exception as e:
            cls.logger.exception(f"Exception occurred while updating is_configured in app.conf on SHC member with uri {uri}. Error: {str(e)}")
            return False

    @classmethod
    def update_app_configured_util(cls, member_object, headers, value, verify=False, cert=None) -> bool:
        if SHClusterUtils.update_app_configured_local_handler(member_object['mgmt_uri'], headers, value, verify, cert):
            return True
        if SHClusterUtils.update_app_configured_local_handler(member_object['mgmt_uri_alias'], headers, value, verify, cert):
            return True

        return False

    @classmethod
    def update_is_configured_in_app_conf(cls, session_key, shc_members, value: bool):
        """
        Update is_configured flag in app.conf for SHC members
        :param session_key:
        :param shc_members:
        :param value:
        :return:
        """
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "Splunk " + session_key
        }

        verify = SHClusterUtils.fetch_root_ca_path()
        cert = SHClusterUtils.fetch_certs()

        cls.logger.info(f"verify {verify}, cert {cert}")

        success_count = 0
        for member_object in shc_members:
            if verify or cert:
                cls.logger.info(f"Updating is_configured (with certs verification) in app.conf on SHC member object {member_object}")
                if SHClusterUtils.update_app_configured_util(member_object, headers, value, verify, cert):
                    success_count += 1
                    continue

            cls.logger.info(f"Updating is_configured (without certs verification) in app.conf on SHC member object {member_object}")
            if SHClusterUtils.update_app_configured_util(member_object, headers, value, False, None):
                success_count += 1
            else:
                cls.logger.error(f"Failed to update is_configured in app.conf on SHC member object {member_object}")

        if success_count == len(shc_members):
            cls.logger.info(f"Successfully updated is_configured in app.conf for all SHC members")
        else:
            raise Exception(messages.ERROR_SETTING_APP_CONFIGURED_IN_SHC_CLUSTER)