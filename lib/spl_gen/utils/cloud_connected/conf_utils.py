import json
import logging
import http.client

import splunk.rest as rest

from spl_gen.constants import SPLUNK_AI_ASSISTANT_CLOUD_APP, SERVICE_INFO_ENDPOINT
from spl_gen.errors import messages

class ConfUtils:

    @classmethod
    def set_logger(cls, logger: logging.Logger):
        cls.logger = logger

    @classmethod
    def set_scs_configs(cls, session_key, configs, conf_name, stanza_name):
        """
        Update SCS Configs with new/updated configuration
        :param session_key:
        :param configs:
        :param stanza_name:
        :return:
        """
        scs_configs_endpoint = f'/servicesNS/nobody/{SPLUNK_AI_ASSISTANT_CLOUD_APP}/properties/{conf_name}/{stanza_name}'
        r, _ = rest.simpleRequest(
            path=scs_configs_endpoint,
            postargs=configs,
            method='POST',
            sessionKey=session_key,
        )
        if r and r.status == http.client.OK:
            cls.logger.info(f"Successfully updated scs config with configs {configs}")
            return

        status_code = r.status if r else None
        cls.logger.error(
            f"{messages.ERROR_SETTING_SCS_CONFIGS}. http_status={status_code}, config={configs}, stanza={stanza_name}")
        raise Exception(messages.ERROR_SETTING_SCS_CONFIGS)

    @classmethod
    def fetch_saia_configs(cls, session_key, stanza_name):
        """
        Returns the configs in the format of:
        {
          <config name>: <config value>,
          ...
        }
        """
        scs_configs_endpoint = f'/servicesNS/nobody/{SPLUNK_AI_ASSISTANT_CLOUD_APP}/properties/splunkaiassistant/{stanza_name}'
        r, c = rest.simpleRequest(
            path=scs_configs_endpoint,
            sessionKey=session_key,
            getargs={'output_mode': 'json'},
        )
        if r.status != http.client.OK:
            raise Exception(f'Failed to fetch configs. stanza={stanza_name}, http_status={r.status}')

        content = json.loads(c).get('entry')
        if content is None:
            raise Exception(f'Empty response when fetching configs. stanza={stanza_name}')

        formatted_configs = {entry['name']: entry.get('content') for entry in content}
        return formatted_configs

    @classmethod
    def update_is_configured_in_app_conf(cls, session_key, value: bool):
        """
        Update is_configured flag in app.conf
        :param session_key:
        :param value:
        :return:
        """
        apps_local_endpoint = f'/servicesNS/nobody/{SPLUNK_AI_ASSISTANT_CLOUD_APP}/apps/local/{SPLUNK_AI_ASSISTANT_CLOUD_APP}'
        r, c = rest.simpleRequest(
            path=apps_local_endpoint,
            postargs={'configured': value},
            method='POST',
            sessionKey=session_key
        )
        if r and r.status == http.client.OK:
            cls.logger.info(f"App marked is_configured={value} successfully in local/app.conf")
            return

        status_code = r.status if r else None
        cls.logger.error(f"{messages.ERROR_SETTING_APP_CONFIGURED}. http_status={status_code}, is_configured={value}")
        raise Exception(messages.ERROR_SETTING_APP_CONFIGURED)

    @classmethod
    def content_present_in_any_entry(cls, configs):
        return configs and any(value != "" for value in configs.values())