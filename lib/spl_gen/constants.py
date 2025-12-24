CUSTOM_CONFIG_FILE_ENDPOINT = "configs/conf-splunkaiassistant/splunk_ai_assistant"
CONFIG_FEATURE_SETTINGS_KEY = "feature_settings"
CONFIG_REMOTE_FEATURES_KEY = "features"
FEATURE_FLAG_KEY_CUSTOMIZATION = "customization"
FEATURE_FLAG_OPTIMIZATION = "optimization"
FEATURE_FLAG_EXTERNAL_LLM_AVAILABLE = 'external_llm_available';

# Saved searches
DATA_ID_SOURCETYPE_METADATA = "sourcetype_metadata"
DATA_ID_INDEXED_FIELDS = "indexed_fields"
DATA_ID_USER_SEARCH_LOGS = "user_search_logs"
DATA_ID_EXTERNAL_LLM = "external_llm"

SAVED_SEARCH_TELEMETRY = "Splunk AI Assistant for SPL - Telemetry"
SAVED_SEARCH_FEEDBACK_TELEMETRY = "Splunk AI Assistant for SPL - Feedback Telemetry"
SAVED_SEARCH_OPEN_IN_SEARCH_TELEMETRY = "Splunk AI Assistant for SPL - Open In Search Telemetry"
AI_SERVICE_DATA_SEARCHES = [
    SAVED_SEARCH_TELEMETRY,
    SAVED_SEARCH_OPEN_IN_SEARCH_TELEMETRY
]
FEEDBACK_SAVED_SEARCHES = [
    SAVED_SEARCH_FEEDBACK_TELEMETRY,
]
SAVED_SEARCH_SEARCH_LOGS_INITIAL = "Splunk AI Assistant for SPL - Search Logs - initial"
SAVED_SEARCH_SEARCH_LOGS = "Splunk AI Assistant for SPL - Search Logs"
SAVED_SEARCH_FIELD_SUMMARY = "Splunk AI Assistant for SPL - Field Summary"
SAVED_SEARCH_INDEXED_FIELDS = "Splunk AI Assistant for SPL - Indexed Fields"
MODULAR_INPUT_FIELD_SUMMARY = ("saia_field_summary", "default")
MODULAR_INPUT_SAIA_ASYNC_JOBS = ("saia_async_jobs", "default")

# Used to manage app side saved search state
SAVED_SEARCH_FEATURE_MAP = {
    FEATURE_FLAG_KEY_CUSTOMIZATION:[SAVED_SEARCH_SEARCH_LOGS, SAVED_SEARCH_INDEXED_FIELDS],
    FEATURE_FLAG_OPTIMIZATION:[],
    FEATURE_FLAG_EXTERNAL_LLM_AVAILABLE: []
}

# Used to manage service side data ID TenantDataConfig state
DATA_CONFIG_FEATURE_MAP = {
    FEATURE_FLAG_KEY_CUSTOMIZATION:[DATA_ID_USER_SEARCH_LOGS, DATA_ID_SOURCETYPE_METADATA],
    FEATURE_FLAG_OPTIMIZATION:[],
    FEATURE_FLAG_EXTERNAL_LLM_AVAILABLE: []
}

MODULAR_INPUT_FEATURE_MAP = {
    FEATURE_FLAG_KEY_CUSTOMIZATION: [MODULAR_INPUT_FIELD_SUMMARY],
}

SAVED_SEARCH_CONFIG_MAP = {
    SAVED_SEARCH_SEARCH_LOGS: {
        "initial_earliest_time": "-1d@d",
        "subsequent_earliest_time": "-1d",
        "default_cron_schedule": "0 0 * * *",
        "cron_schedule_mins_offset": 0,
        "data_id": DATA_ID_USER_SEARCH_LOGS
    },
    SAVED_SEARCH_INDEXED_FIELDS: {
        "initial_earliest_time": "-1m",
        "subsequent_earliest_time": "-1m",
        "default_cron_schedule": "30 4 * * 0",
        "cron_schedule_mins_offset": 0,
        "data_id": DATA_ID_INDEXED_FIELDS
    },
    # SAVED_SEARCH_FIELD_SUMMARY: {
    #     "initial_earliest_time": "-1d@d",
    #     "subsequent_earliest_time": "-1d",
    #     "default_cron_schedule": "0 0 * * *",
    #     "cron_schedule_mins_offset": 20,
    #     "data_id": DATA_ID_SOURCETYPE_METADATA
    # }
}

# Needed for modinput functioning
GET = "GET"
POST = "POST"

INFRA_CONF_ENDPOINT = "/servicesNS/nobody/Splunk_AI_Assistant_Cloud/configs/conf-infra/fedramp"

MODINPUT_NAMESPACE = "saia_modinput_file_namespace"

FILE_LOCK_DIRECTORY = "file_locks"

WINDOWS_SYS_PLATFORM = ["win32", "cygwin", "msys"]

SPLUNK_AI_ASSISTANT_CLOUD_APP = 'Splunk_AI_Assistant_Cloud'

SERVICE_INFO_ENDPOINT = '/services/server/info'

SAIA_REALM = 'saia'
SAIA_CONF_FILE = 'splunkaiassistant'
SAIA_SCS_STANZA = 'cloud_connected_configurations'
SAIA_CMP_ONBOARD_OTP_LENGTH = 6

SAIA_KEY_ROTATION_THRESHOLD_IN_DAYS = 85
SCS_DOMAIN = 'scs.splunk.com'

SAIA_CONFIGURATION = 'is_configured'
SAIA_CONFIGURATION_FILE = 'app'
SAIA_CONFIGURATION_STANZA = 'install'

SAIA_PRIVATE_KEY = 'private_key'
SAIA_PUBLIC_JWK = 'public_jwk'

SAIA_UI_CONFIGURATION_STANZA = 'ui'
SAIA_SETUP_VIEW = 'setup_view'

SAIA_PROXY_SETTINGS_STANZA = 'cloud_connected_configurations:proxy_settings'
SAIA_PROXY_PASSWORD = 'proxy_password'

STOPPING_CHUNK_SEQ = "###STOPPING_CHUNK###"