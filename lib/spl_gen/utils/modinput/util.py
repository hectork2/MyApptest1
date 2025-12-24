import json
import sys
try:
    import splunk
    from splunk import rest
    import splunk.util as sp_util
except:
    print("Warning: Could not import splunk python package (requires Splunk be installed)", file=sys.stderr)

from requests import codes


from ...constants import GET, POST

ENABLE_MOD_INPUT_ACTION = "enable"
DISABLE_MOD_INPUT_ACTION = "disable"


def is_mod_input_enabled(logger, session_key, mod_input, stanza_name):
    """Get modular input state and enable if required

    Args:
        logger: Instance of logger
        session_key: Valid session key
        mod_input: Name of the modular input
        stanza_name: Name of the mod input stanza.

    Returns:
        True: If modinput is already enabled or successfully enabled
        False: Otherwise

    """

    get_mod_input_api = f"/servicesNS/nobody/Splunk_AI_Assistant_Cloud/data/inputs/{mod_input}/{stanza_name}"

    try:
        response, content = rest.simpleRequest(
            get_mod_input_api, getargs={"output_mode": "json"}, sessionKey=session_key, method=GET
        )

        if response.status == 200:
            content = json.loads(content)
            current_state = sp_util.normalizeBoolean(
                content.get("entry", [{}])[0].get("content", {}).get("disabled", "true")
            )

            return not current_state
        return False
    except splunk.ResourceNotFound:
        logger.exception(
            f"ResourceNotFound for Mod-input {mod_input} and stanza {stanza_name} exception"
        )
        return False
    except Exception:
        logger.exception(
            f"check_and_enable_mod_input failed for Mod-input {mod_input} and stanza {stanza_name}"
        )
        return False


def update_mod_input(logger, session_key, mod_input, stanza_name, operation=ENABLE_MOD_INPUT_ACTION):
    """Update modular input. Can we used to enable or disable any Mission Control
    modular input.

    Args:
        logger: Instance of logger
        session_key: Valid session key
        mod_input: Name of the modular input
        stanza_name: Name of the mod input stanza.
        operation: enable or disable
    """
    mod_input_api = f"/servicesNS/nobody/Splunk_AI_Assistant_Cloud/data/inputs/{mod_input}/{stanza_name}/{operation}"

    logger.info(
        "Updating modular input. mod_input=%s, stanza=%s, operation=%s",
        mod_input,
        stanza_name,
        operation,
    )

    try:
        response, content = rest.simpleRequest(
            mod_input_api, postargs={"output_mode": "json"}, sessionKey=session_key, method=POST
        )
        if response.status == codes.ok:
            logger.info(
                "Successfully updated modular input. mod_input=%s, stanza=%s, operation=%s",
                mod_input,
                stanza_name,
                operation,
            )
            return True

        logger.error(
            "Error updating modular input mod_input=%s, stanza=%s. status=%s, error=%s",
            mod_input,
            stanza_name,
            response.status,
            content,
        )
        return False
    except Exception as e:
        logger.exception("Exception updating modular input. error=%s", e)
        raise e

