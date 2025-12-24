import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

from splunklib.client import connect
from splunklib.searchcommands import environment
from spl_gen.scs_utils import ScsUtils
from spl_gen.utils.cloud_connected.conf_utils import ConfUtils
from spl_gen.constants import SPLUNK_AI_ASSISTANT_CLOUD_APP, SAIA_SETUP_VIEW, SAIA_CONFIGURATION_FILE, SAIA_UI_CONFIGURATION_STANZA
from spl_gen.errors.messages import FAILED_RELOADING_APP
from spl_gen.utils import reload_saia_app

if __name__ == "__main__":

    environment.app_root = os.path.join(os.path.dirname(__file__), "..")
    logger, _ = environment.configure_logging('check_saia_cloud_connected.py')
    ScsUtils.set_logger(logger)
    ConfUtils.set_logger(logger)

    try:
        session_key = sys.stdin.readline().strip()
        logger.info("installing saia app")
        if not ScsUtils.is_cloud_stack(session_key):
            logger.info("Creating a new connection object")
            service = connect(
                app=SPLUNK_AI_ASSISTANT_CLOUD_APP,
                token=session_key
            )
            # Update setup_view configuration for saia cmp
            ConfUtils.set_scs_configs(session_key, {SAIA_SETUP_VIEW: 'setup'}, SAIA_CONFIGURATION_FILE, SAIA_UI_CONFIGURATION_STANZA)
            # Reload saia app to update confs
            if not reload_saia_app(service):
                raise Exception(FAILED_RELOADING_APP)
            logger.info("Successfully updated setup view configuration")
    except Exception as e:
        logger.exception(f"Error: {str(e)}")