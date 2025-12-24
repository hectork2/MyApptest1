import json
import re
import time
import logging
import hashlib
from splunklib.client import Service, connect
from splunklib.binding import handler
from datetime import datetime
from ..constants import STOPPING_CHUNK_SEQ, SAVED_SEARCH_FEATURE_MAP, SAVED_SEARCH_CONFIG_MAP, FEEDBACK_SAVED_SEARCHES, AI_SERVICE_DATA_SEARCHES, MODULAR_INPUT_FEATURE_MAP
from .modinput.util import is_mod_input_enabled, update_mod_input, ENABLE_MOD_INPUT_ACTION, DISABLE_MOD_INPUT_ACTION
from ..errors import messages

import splunk.rest as rest
import requests

def clean_chunks(decoded_chunk):
    if STOPPING_CHUNK_SEQ in decoded_chunk:
        decoded_chunk = decoded_chunk.split(STOPPING_CHUNK_SEQ)[0]

    return decoded_chunk

def log_kwargs(**kwargs):
    return ", ".join(f'{key}="{value}"' for key, value in kwargs.items())

def deterministic_hash(input_string):
    """
    Create a deterministic hash of a string that will be consistent across Python sessions.

    Args:
        input_string (str): The string to hash

    Returns:
        int: A deterministic integer hash that's unique per input and consistent across sessions
    """
    if input_string is None:
        input_string = ""
    # Use SHA-256 for deterministic hashing, then convert to integer
    hex_hash = hashlib.sha256(input_string.encode('utf-8')).hexdigest()
    # Convert the first 16 characters of the hex to an integer to get a reasonable size
    # This gives us a 64-bit integer which is similar to Python's hash() output size
    return int(hex_hash[:16], 16)

def get_chat_history_obj(response):
    if isinstance(response["chat_history"], str):
        chat_history_obj = json.loads(response["chat_history"])
    else:
        chat_history_obj = response["chat_history"]
    return chat_history_obj

def read_splk_content(response):
    body = response["body"].read()
    try:
        json_res = json.loads(body)
        entry = json_res.get("entry")[0]
        return entry.get("content")
    except json.JSONDecodeError:
        raise RuntimeError(f"Unable to parse json entry: {body}")

def get_default_record_map():
    return {
        "threadName": f"New chat",
        "lastModified": time.time() * 1000,
        "assistantMode": "write",
        "empty": True,
        "records": {
            "write": [],
            "explain": [],
            "tellme": []
        }
    }

def get_classification_string_for_code(code):
    classification_map = {
        0: "write",
        1: "explain",
        2: "tellme",
    }

    return classification_map.get(code)

def get_classification_code_from_orchestrator(res):
    if "write_spl" in res:
        return 0
    elif "explain_spl" in res:
        return 1
    else:
        raise RuntimeError("Unsupported classification from orchestrator {res}")

def get_telemetry_string_for_classification(code):
    classification_map = {
        0: "write",
        1: "explain",
        2: "tellme",
        3: "optimize",
    }

    return classification_map.get(code)

def get_ai_usage_dashboard_string_for_classification(code):
    classification_map = {
        0: "Write SPL",
        1: "Explain SPL",
        2: "Tell me about",
        3: "Optimize SPL",
        4: "Parser feedback",
        5: "Sourcetype Disambiguation",
        6: "Clarification",
    }

    return classification_map.get(code)

def cleanup_chat_history(chat_history):
    for key, value in chat_history.items():
        cleaned_chat = cleanup_chat_history_chat(value)
        chat_history[key] = cleaned_chat

    return chat_history

def cleanup_chat_history_chat(chat_history_chat):
    all_empty = True
    for assistant_mode, thread in chat_history_chat["records"].items():
        cleaned_thread = cleanup_chat_thread(thread)
        chat_history_chat["records"][assistant_mode] = cleaned_thread
        if (len(cleaned_thread) != 0):
            all_empty = False
    chat_history_chat["empty"] = all_empty
    return chat_history_chat

def cleanup_chat_thread(chat_thread):
    new_thread = []
    for item in chat_thread:
        # Rename type as role
        if item.get("role") is None and item.get("type") is not None:
            item["role"] = item["type"]
        if item.get("loadingState") is None:
            item["loadingState"] = 2  # Migration of older chat entries - mark them all as done
        # Rename copilot role as assistant
        if item.get("role") == "copilot":
            item["role"] = "assistant"

        # Filter out error and empty entries
        if item.get("role") == "assistant" and item.get("loadingState") is not None:
            if item.get("loadingState") != 2 or item.get("stopped", False):
                # This is a mangled response entry, delete it and corresponding prompt from history
                if new_thread[-1].get("role") == "user":
                    new_thread.pop()
                continue
        new_thread.append(item)
    return new_thread

def process_response_header_for_metadata(response, metadata, update_chat_title):
    for key, value in response.headers.items():
        key = key.lower()
        if key.startswith("warning-"):
            metadata["warnings"].append(value)
        if key == "metadata-source-urls":
            metadata["sourceUrls"] = json.loads(value)
        if key == "metadata-source-titles":
            metadata["sourceTitles"] = json.loads(value)
        if key == "metadata-chat-title-summary":
            update_chat_title(value)
            pass
        if key == "notify-reload-required":
            metadata["reloadRequired"] = value
    return metadata

class SimpleError(Exception):
    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return self.message

def unwrap_runtime_error(err):
    if (len(err.args)
        and err.args[0]):
        if (isinstance(err.args[0], dict) and "error" in err.args[0]):
            return err.args[0]["error"]
        else:
            return err.args[0]
    else:
        return repr(err)

def find_saved_search(service: Service, search_name: str):
    logger = logging.getLogger("SavedSearchRetrieval")
    try:
        return service.saved_searches[search_name]
    except Exception as e:
        logger.error(f"An exception occured while retrieving saved search {search_name}: {repr(e)}")
        return None

def reset_saved_search_time(service: Service, search_name: str):
    saved_search = find_saved_search(service, search_name) # pyright: ignore
    if not saved_search:
        raise Exception("Saved search not found")

    saved_search_config = SAVED_SEARCH_CONFIG_MAP[search_name]
    kwargs = {"dispatch.earliest_time": saved_search_config["subsequent_earliest_time"]} # pyright: ignore
    saved_search.update(**kwargs).refresh()

def get_updated_cron_schedule(default_schedule: str, mins_offset: int = 0):
    """Update the specified components of the default schedule to align with the current time"""
    now = datetime.now()

    mins_val = now.minute
    hours_val = now.hour

    # Apply offset from current time and handle case of shift into previous hour
    mins_val -= mins_offset
    if mins_val < 0:
        mins_val += 60
        hours_val -= 1

    if hours_val < 0:
        hours_val += 24

    # Need to align with cron time/date format
    current_vals = [mins_val, hours_val, now.day - 1, now.month - 1, (now.weekday() + 1) % 7]

    default_vals = default_schedule.split()

    updated_schedule = []
    for default_val, current_val in zip(default_vals, current_vals):
        if default_val == "*":
            updated_schedule.append(default_val)
        else:
            updated_schedule.append(str(current_val))

    return " ".join(updated_schedule)

def update_saved_searches_from_setting(saved_searches: list, service: Service, setting_val: bool):
    for saved_search_name in saved_searches:
        saved_search = find_saved_search(service, saved_search_name)
        if saved_search is None:
            continue

        existing_enabled_state = saved_search["disabled"] == "0"
        new_enabled_state = setting_val
        update_required = existing_enabled_state != new_enabled_state
        if not update_required:
            continue

        try:
            if new_enabled_state:
                saved_search.enable()
            else:
                saved_search.disable()
        except Exception:
            pass

def update_saved_searches_from_ai_service_data_enabled_setting(ai_service_data_enabled: bool, service: Service):
    update_saved_searches_from_setting(
        AI_SERVICE_DATA_SEARCHES,
        service,
        ai_service_data_enabled
    )

def update_modular_inputs_from_feature_settings(feature_settings: dict, service: Service):
    logger = logging.getLogger("UpdateModularInputsFromFeatureSettings")
    for feature_key, modular_input_names in MODULAR_INPUT_FEATURE_MAP.items():
        for modular_input in modular_input_names:
            modular_input_name = modular_input[0]
            modular_input_stanza = modular_input[1]
            existing_enabled_state = is_mod_input_enabled(
                logger,
                service.token,
                modular_input_name,
                modular_input_stanza
            )
            new_enabled_state = feature_settings.get(feature_key, {}).get("enabled") or False
            update_required = existing_enabled_state != new_enabled_state
            if not update_required:
                continue

            try:
                if new_enabled_state:
                    action = ENABLE_MOD_INPUT_ACTION
                else:
                    action = DISABLE_MOD_INPUT_ACTION
                update_mod_input(
                    logger,
                    service.token,
                    modular_input_name,
                    modular_input_stanza,
                    action
                )
            except Exception:
                logger.warning("Failed to update modular input")
                pass


def update_saved_searches_from_feature_settings(feature_settings: dict, service: Service, data_to_refresh = None):
    if data_to_refresh is None:
        data_to_refresh = []

    for feature_key, saved_search_names in SAVED_SEARCH_FEATURE_MAP.items():
        for saved_search_name in saved_search_names:
            saved_search = find_saved_search(service, saved_search_name)
            if saved_search is None:
                continue

            saved_search_config = SAVED_SEARCH_CONFIG_MAP[saved_search_name]

            existing_enabled_state = saved_search["disabled"] == "0"
            new_enabled_state = feature_settings.get(feature_key, {}).get("enabled") or False
            update_required = existing_enabled_state != new_enabled_state
            if not update_required:
                continue

            try:
                if new_enabled_state:
                    saved_search.enable()

                    # Update the saved search cron schedule to align with the current time
                    cron_schedule = get_updated_cron_schedule(
                        saved_search_config["default_cron_schedule"],
                        saved_search_config["cron_schedule_mins_offset"]
                    )
                    kwargs = {"cron_schedule": cron_schedule}

                    should_dispatch = False
                    if saved_search_config["data_id"] in data_to_refresh:
                        should_dispatch = True
                        kwargs["dispatch.earliest_time"] = saved_search_config["initial_earliest_time"]

                    saved_search.update(**kwargs).refresh()
                    if should_dispatch:
                        saved_search.dispatch()
                else:
                    saved_search.disable()
            except Exception:
                pass

def get_updated_feature_settings(
    existing_feature_settings: dict,
    remote_enabled_features: list,
):
    updated_feature_settings = {}
    for feature_key in remote_enabled_features:
        if feature_key in existing_feature_settings:
            updated_feature_settings[feature_key] = {**existing_feature_settings[feature_key]}
        else:
            updated_feature_settings[feature_key] = {
                "enabled": False,
            }

    return updated_feature_settings

def get_enabled_feature_flags(feature_settings: dict):
    enabled_flags = []
    for flag_key, value in feature_settings.items():
        if value.get("enabled", False):
            enabled_flags.append(flag_key)
    return enabled_flags

# Builds a warning for allowable AST parsing failures
# those being, missing datamodel, missing macro, missing lookup
def get_allowable_ast_parsing_warning(error_body):
    failure_reason = None
    if re.search(
        r".*The search specifies a macro .* that cannot be found.*",
        error_body,
    ):
        failure_reason = "macro"
    elif re.search(
        r".*Error in \'DataModelEvaluator\': Data model .* was not found.*",
        error_body,
    ):
        failure_reason = "data model"
    elif re.search(
        r".*Error in \'lookup\' command: Could not construct lookup.*",
        error_body,
    ):
        failure_reason = "lookup"

    if failure_reason == None:
        return None
    else:
        return f"Missing {failure_reason} was detected when parsing query."

def get_deployment_id(system_scoped_service):
    # Get a service with correct scope (splunk_instrumentation)
    service = connect(
        token=system_scoped_service.token,
        handler=handler(timeout=1),
        host="127.0.0.1",
        app="splunk_instrumentation",
        owner="splunk-system-user",
        retries=2
    )
    telemetry_conf = service.confs["telemetry"]

    return telemetry_conf.list()[0].content['deploymentID']

def get_app_version(service):
    return service.apps["Splunk_AI_Assistant_Cloud"].content['version']

OPENING_DELIMITER = "```splunk-spl"
CLOSING_DELIMITER = "```"
def detect_spl_block(text):
    locations = []
    index = 0
    in_spl_block = False

    while True:
        if not in_spl_block:
            start = text.find(OPENING_DELIMITER, index)
            if start == -1:
                break
            in_spl_block = True
            locations.append(start + len(OPENING_DELIMITER))
            index = start + len(OPENING_DELIMITER)
        elif in_spl_block:
            # If it's another delimiter, remove it
            end = text.find(CLOSING_DELIMITER, index)
            if end == -1:
                break
            locations.append(end)
            in_spl_block = False
            index = index + len(CLOSING_DELIMITER)
    return locations

def get_spl(response_str, locations):
    spl = response_str[locations[0]:locations[1]-1]
    spl = spl.strip()
    if not spl.startswith("|") and not spl.startswith(
        "search"
    ):
        spl = "search " + spl
    print(spl)
    # Replace single angle bracket placeholders with double angle bracket placeholders
    result = re.sub(r"=<<(.*?)>>", r"=<\1>", spl)
    result = re.sub(r"=<(.*?)>", r"=<<\1>>", result)
    return result

def fetch_valid_licenses(entry_array):
    licenses = []
    for entry in entry_array:
        content = entry['content']
        expiration_time = content['expiration_time']
        licenses.append({
            'license_guid': content['guid'],
            'group_id': content['group_id'],
            'name': entry['name'],
            'expiration_time': datetime.fromtimestamp(expiration_time).strftime('%Y-%m-%d %H:%M:%S %Z')
        })
    return licenses

def fetch_splunk_license(session_key):
    logger = logging.getLogger("FetchSplunkLicense")
    base_url = rest.makeSplunkdUri()
    url = '{}/services/licenser/licenses'.format(base_url)
    params = {
        'output_mode': 'json'
    }
    try:
        response, content = rest.simpleRequest(
            url,
            sessionKey=session_key,
            getargs=params,
            method='GET',
            raiseAllErrors=True
        )
        parsed = json.loads(content)
        if 'entry' in parsed and len(parsed['entry']) > 0:
            return fetch_valid_licenses(parsed['entry'])
        else:
            logger.error(messages.NO_VALID_LICENSE_FOUND)
            return None
    except Exception as e:
        logger.error(f"{messages.ERROR_FETCHING_LICENSES} : {str(e)}")
        raise

# storing secret in passwords.conf file
def store_secret_key(service: Service, secret_value, secret_key, user_name, delete_before_store = False):
    try:
        if delete_before_store:
            delete_secret_key(service, secret_key, user_name)

        service.storage_passwords.create(
                password=secret_value, username=user_name, realm=secret_key
            )
    except Exception as e:
        return e

# Fetching secret from passwords.conf file
def get_secret_key(service: Service, secret_key, user_name, raise_on_error=False):
    logger = logging.getLogger("GetSecretKey")
    try:
        secret = service.storage_passwords[f"{secret_key}:{user_name}"]
        return secret.clear_password
    except Exception as e:
        logger.info(f"get secret failed with {e}")
        if raise_on_error:
            raise
        return e

# Delete secret from passwords.conf file
def delete_secret_key(service: Service, secret_key, user_name):
    try:
        service.storage_passwords.delete(
            username=user_name, realm=secret_key
        )
    except Exception as e:
        return e

# Reload SAIA App to update .conf files
def reload_saia_app(service: Service):
    logger = logging.getLogger("ReloadSAIAApp")
    try:
        r, _ =  rest.simpleRequest('apps/local/_reload', sessionKey=service.token)
        logger.info(f"reload successful")
        return True
    except Exception as e:
        logger.error(f"reload failed with {e}")
        return False
