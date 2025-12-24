import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

from splunklib.client import connect
from splunklib.searchcommands import environment
from spl_gen.scs_utils import ScsUtils
from spl_gen.constants import SPLUNK_AI_ASSISTANT_CLOUD_APP

if __name__ == "__main__":

    environment.app_root = os.path.join(os.path.dirname(__file__), "..")
    logger, _ = environment.configure_logging('maintenance_key_rotation.py')
    ScsUtils.set_logger(logger)

    try:
        session_key = sys.stdin.readline().strip()

        if ScsUtils.is_cloud_stack(session_key) or ScsUtils.is_non_captain_in_shcluster_setup(session_key):
            logger.info("Skipping key rotation on Cloud Stack or SH non-captain node in SH cluster setup")
        else:
            logger.info("Creating a new connection object")
            service = connect(
                app=SPLUNK_AI_ASSISTANT_CLOUD_APP,
                token=session_key
            )

            ScsUtils.rotate_public_private_keys(service)
            logger.info("Successfully executed cron task")
    except Exception as e:
        logger.exception(f"Error: {str(e)}")