import json
import time
import logging
import typing
from ..tools import ToolRequest
from ..remote.v1alpha1 import SaiaApi
from ..utils import clean_chunks, log_kwargs, process_response_header_for_metadata
from ..saia_collection import SaiaCollection

# V2 of write_spl skill, yields into a scratchpad (allows for rewrites)
def run_inference_func(tool_request: ToolRequest):
    logger = logging.getLogger(__name__)
    logger.info(
        log_kwargs(
            message="Inside run_inference_func",
            **tool_request.logging_context,
        )
    )
    start_time = time.time()
    entry_to_update = {}
    # Create scratchpad entry
    tool_request.scratchpad.insert(tool_request.job_id, entry_to_update)

    api = SaiaApi(tool_request.service, tool_request.system_scoped_service, tool_request.user, tool_request.chat_id, tool_request.logging_context["hashed_user"])

    metadata: dict[str, typing.Any] = {"warnings": []}
    tool_request.kwargs["job_id"] = tool_request.job_id
    # Stream and yield into tool scratchpad
    response = api.inference(
        job_id=tool_request.job_id,
        system_prompt=tool_request.kwargs["system_prompt"],
        chat_history=tool_request.kwargs["chat_history"],
        log_to_telemetry=tool_request.logging_context["should_log_telemetry"],
        source_app_id=tool_request.logging_context["source_app_id"],
    )

    metadata = process_response_header_for_metadata(response=response, metadata=metadata, update_chat_title=lambda *args: None)

    entry_to_update["metadata"] = metadata
    partial_text = ""
    entry_to_update["loadingState"] = 1  # 0 - loading, 1 - streaming, 2 - complete, 3 - error, 4 - stopped

    first_token_received = False
    for text in response.iter_content(chunk_size=None):
        if not first_token_received:
            first_token_received = True
            first_token_time = time.time()
            logger.info(
                log_kwargs(
                    time_to_first_token=round((first_token_time - start_time), 5),
                    **tool_request.logging_context,
                )
            )
        decoded_chunk = text.decode('utf-8', 'ignore')  # pyright: ignore
        decoded_chunk = clean_chunks(decoded_chunk)
        if decoded_chunk.startswith("### ERROR "):
            err_message = decoded_chunk.split("### ERROR ")[1]
            # SAIA-737: Streaming errors need to be handled at a higher level w/ our own error format
            # since requests library doesn't support HTTP trailers for this use case
            raise RuntimeError({"error": err_message})
        partial_text += decoded_chunk
        entry_to_update["content"] = partial_text
        tool_request.scratchpad.update(tool_request.job_id, entry_to_update, "running")


    entry_to_update["loadingState"] = 2  # 0 - loading, 1 - streaming, 2 - complete, 3 - error, 4 - stopped
    entry_to_update["content"] = partial_text
    tool_request.scratchpad.update(tool_request.job_id, entry_to_update, "finished")
    logger.info(
        log_kwargs(
            message="Finished run_inference_func tool invocation",
            **tool_request.logging_context,
        )
    )
