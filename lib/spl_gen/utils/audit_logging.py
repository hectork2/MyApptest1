import json
import socket

from ..metadata.collection import MetadataCollection
from ..constants import SPLUNK_AI_ASSISTANT_CLOUD_APP, FEATURE_FLAG_EXTERNAL_LLM_AVAILABLE

def generate_chat_audit_log(
    service,
    request_id,
    chat_id,
    user,
    role,
    content,
):
    index = service.indexes["_audit"]
    sourcetype = "splunk_ai_assistant_chat_log"
    host = service.host
    request_id = request_id
    metadata_collection = MetadataCollection(service)
    enabled_feature_flags, _ = metadata_collection.get_enabled_feature_flags()

    event = {
        "request_id": request_id,
        "chat_id": chat_id,
        "user": user,
        "role": role,
        "content": content,
        "third_party_llm_enabled": FEATURE_FLAG_EXTERNAL_LLM_AVAILABLE in enabled_feature_flags
    }

    event_text = json.dumps(event)
    index.submit(
        event_text,
        sourcetype=sourcetype,
        host=socket.gethostname(),
    )


def generate_ai_service_usage_log(
    service,
    request_id,
    time,
    user,
    prompt_types,
    status,
    source_app_id
):
    index = service.indexes["_internal"]
    sourcetype = "ai_event_log"
    host = service.host
    event_type = "ai_usage"
    app = source_app_id
    metadata_collection = MetadataCollection(service)
    enabled_feature_flags, _ = metadata_collection.get_enabled_feature_flags()

    event = {
        "time": time,
        "type": event_type,
        "app": app,
        "user": user,
        "prompt_types": prompt_types,
        "prompt_rid": request_id,
        "status": status,
        "third_party_llm_enabled": FEATURE_FLAG_EXTERNAL_LLM_AVAILABLE in enabled_feature_flags
    }

    event_text = json.dumps(event)

    index.submit(
        event_text,
        sourcetype=sourcetype,
        host=socket.gethostname(),
    )
