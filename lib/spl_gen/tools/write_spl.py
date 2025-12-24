import json
import concurrent.futures
import time
import logging
import typing
from ..metadata.collection import MetadataCollection
from splunklib.binding import HTTPError as SplunkHTTPError
from ..tools import ToolRequest
from ..remote.v1alpha1 import SaiaApi
from ..utils import clean_chunks, log_kwargs, process_response_header_for_metadata, detect_spl_block, get_ai_usage_dashboard_string_for_classification, get_spl, get_allowable_ast_parsing_warning, cleanup_chat_thread
from ..utils.audit_logging import generate_chat_audit_log, generate_ai_service_usage_log
from ..saia_collection_v2 import SaiaCollectionV2

NO_APPLICABLE_OPTIMIZATIONS_STR = "### NO APPLICABLE OPTIMIZATIONS"

def check_spl_with_parser_with_timeout(input_string, tool_request, logger, timeout=5):
    spl = input_string
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(check_spl_with_parser, input_string, tool_request, logger)
        try:
            # Wait for the result within the timeout
            spl, is_parsable, parsing_error, warning = future.result(timeout=timeout)
            return spl, is_parsable, parsing_error, warning
        except concurrent.futures.TimeoutError:
            logger.warning(
                log_kwargs(
                    message="Warning: timed out running parser check, proceeding with assumption that SPL is valid!",
                )
            )
            return spl, True, None, None
        except Exception as e:
            logger.warning(
                log_kwargs(
                    message="Warning: encountered exception during parser check in check_spl_with_parser_with_timeout",
                    error=e
                )
            )
            return spl, True, None, None

def check_spl_with_parser(input_string, tool_request, logger):
    spl = None
    locations = detect_spl_block(input_string)
    if len(locations) >= 2:
        try:
            spl = get_spl(input_string, locations)
            _ = tool_request.service.parse(query=spl, output_mode="json")["body"].read()
            return spl, True, None, None
        except SplunkHTTPError as e:
            if e.status == 400:
                # Allow queries with missing macros, lookups, or data models to still be explained
                if (
                    e.body
                ):
                    allowable_warning = get_allowable_ast_parsing_warning(e.body.decode("utf-8"))
                    if allowable_warning is not None:
                        return spl, True, None, None
            return spl, False, e, None
        except Exception as e:
            warning_message = "Encountered an error running the SPL parser, SPL is not validated"
            logger.warning(
                log_kwargs(
                    message="Warning: encountered exception during parser check",
                    error=e
                )
            )
            return spl, True, None, warning_message
    return spl, False, None, None

# V2 of write_spl skill, yields into a scratchpad (allows for rewrites)
def write_spl_tool_func(tool_request: ToolRequest):
    logger = logging.getLogger(__name__)
    logger.info(
        log_kwargs(
            message="Inside write_spl_tool_func",
            **tool_request.logging_context,
        )
    )
    start_time = time.time()

    generate_chat_audit_log(
        tool_request.system_scoped_service,
        request_id=tool_request.job_id,
        chat_id=tool_request.chat_id,
        user=tool_request.user,
        role="user",
        content=tool_request.kwargs["user_prompt"].strip(),
    )

    entry_to_update = {}

    saia_collection_v2 = SaiaCollectionV2(tool_request.service, tool_request.user)

    metadata_collection = MetadataCollection(tool_request.service)

    saia_feature_metadata = metadata_collection.get()
    orchestration_enabled = saia_feature_metadata[MetadataCollection.KEY_ORCHESTRATOR_ENABLED] # type: ignore

    logger.info(
        log_kwargs(
            saia_feature_metadata=saia_feature_metadata,
            message=f"Orchestration enabled: {orchestration_enabled}",
            **tool_request.logging_context,
        )
    )

    ret = saia_collection_v2.get_chat(tool_request.chat_id)
    if ret is not None:
        (chat_key, chat_history_obj) = ret
    else:
        # We should not get here!
        logger.warning(
            log_kwargs(message="Chat ID not found. This should not happen!", **tool_request.logging_context)
        )
        return

    api = SaiaApi(tool_request.service, tool_request.system_scoped_service, tool_request.user, tool_request.chat_id, tool_request.logging_context["user"])

    metadata: dict[str, typing.Any] = {"warnings": []}
    tool_request.kwargs["job_id"] = tool_request.job_id
    chat_history = chat_history_obj["records"]["write"]

    resp_index = next((i for i, item in enumerate(chat_history) if item["id"] == tool_request.job_id), -1)
    entry_to_update = chat_history[resp_index]
    logger.info(chat_history)
    chat_history_in = chat_history[0:resp_index]
    logger.info(chat_history_in)
    user_prompt = tool_request.kwargs["user_prompt"].strip()
    # Orchestration call
    classification = 0
    indexed_fields = []
    ast = ""

    orchestration_response = api.make_orchestration_request(
        job_id=tool_request.job_id,
        user_prompt=user_prompt,
        chat_history=json.dumps(chat_history_in),
        classification=0,
        locale="en-US",
        log_to_telemetry=tool_request.logging_context["should_log_telemetry"],
        was_chat_empty=chat_history_obj["threadName"] == "New chat",
        source_app_id=tool_request.logging_context["source_app_id"],
        ast="",
        rag_data_only=False,
        rewrite_content=True
    )

    orchestration_response_json = orchestration_response.json()

    classification = orchestration_response_json["classification"]

    if classification == 6 or classification == 7: # 6 - Prompt Injection Detected, 7 - Out of Scope Query Detected
        # Special case, clarification needed, return the clarification as assistant response turn
        if "content" in orchestration_response_json:
            entry_to_update["content"] = orchestration_response_json["content"]
        elif "followup_message" in orchestration_response_json:
            entry_to_update["content"] = orchestration_response_json["followup_message"]
        else:
            logger.info("Warning: clarification needed but no content provided, using default message")
            entry_to_update["content"] = "Sorry, can you try rephrasing? Changing a word or two can generate a better result."
        entry_to_update["loadingState"] = 2  # 0 - loading, 1 - streaming, 2 - complete, 3 - error, 4 - stopped
        saia_collection_v2.update(key=chat_key, chat_history=chat_history_obj)
        end_time = time.time()
        logger.info(
            log_kwargs(
                apply_time=round((end_time - start_time), 5),
                **tool_request.logging_context,
            )
        )
        logger.info(
            log_kwargs(
                message="Finished write_spl tool invocation with clarification",
                **tool_request.logging_context,
            )
        )
        generate_chat_audit_log(
            tool_request.system_scoped_service,
            request_id=tool_request.job_id,
            chat_id=tool_request.chat_id,
            user=tool_request.user,
            role="assistant",
            content=orchestration_response_json["content"],
        )
        generate_ai_service_usage_log(
            tool_request.system_scoped_service,
            request_id=tool_request.job_id,
            time=end_time,
            user=tool_request.user,
            prompt_types=[get_ai_usage_dashboard_string_for_classification(classification)],
            status=200,
            source_app_id=tool_request.logging_context["source_app_id"],
        )
        return

    # For Explain SPL or Optimize SPL intent (classification==1 or classification==3), we want to pass in the parsed candidate SPL
    if classification == 3 or classification == 1:
        user_prompt = orchestration_response_json["content"]
        # A nasty bit of code to get the ast string in a deserializable format that SAIA API understands
        try:
            ast = json.dumps(
                json.loads(
                    tool_request.service.parse(query=prompt_to_parse, output_mode="json")[ # type: ignore
                        "body"
                    ].read()
                )
            )
        except:
            logger.warning("Warning: failed to parse SPL candidate")

        # Fetch indexed fields for optimization request
        if orchestration_response_json["classification"] == 3:
            saia_collection_data = tool_request.system_scoped_service.kvstore["saia_indexed_fields"]
            indexed_fields = saia_collection_data.data.query()
            for indexed_field in indexed_fields:
                del indexed_field["_key"]
                del indexed_field["_user"]

    response = api.search(
        job_id=tool_request.job_id,
        user_prompt=user_prompt,
        chat_history=json.dumps(chat_history_in),
        classification=classification,
        locale="en-US",
        log_to_telemetry=tool_request.logging_context["should_log_telemetry"],
        was_chat_empty=chat_history_obj["threadName"] == "New chat",
        source_app_id=tool_request.logging_context["source_app_id"],
        ast=ast,
        rag_data_only=False,
        rewrite_content=True,
        indexed_fields=indexed_fields,
        use_state_streamer=orchestration_enabled,
    )

    def update_chat_title(value):
        # For previously new/empty chats, update thread with chat title summary
        # Headers are decoded assuming latin-1 by default, but we need to use UTF-8 to support multiple languages
        chat_history_obj["threadName"] = value.encode('latin1').decode('unicode-escape')
        saia_collection_v2.update(key=chat_key, chat_history=chat_history_obj)
    metadata = process_response_header_for_metadata(response=response, metadata=metadata, update_chat_title=update_chat_title)

    entry_to_update["metadata"] = metadata
    entry_to_update["rewrite_content"] = True
    partial_text = ""
    entry_to_update["loadingState"] = 1  # 0 - loading, 1 - streaming, 2 - complete, 3 - error, 4 - stopped

    first_token_received = False
    spl = ""
    parser_error = None
    parser_validated = False

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

        if orchestration_enabled:
            try:
                chunk_json = json.loads(decoded_chunk.split("data: ")[1])
                if 'reasoning' in chunk_json:
                    entry_to_update['reasoning'] = chunk_json['reasoning']
                # Prefer explicit content field if present
                if 'content' in chunk_json and isinstance(chunk_json['content'], str):
                    partial_text = chunk_json['content']
                    partial_text = clean_chunks(partial_text)
                    entry_to_update['content'] = partial_text
            except Exception as e:
                logger.warning(
                    log_kwargs(
                        message="Warning: Failed to parse JSON chunk from streaming response",
                        error=str(e),
                        **tool_request.logging_context,
                    )
            )
                # Fallback to setting the decoded chunk as the content
                partial_text = decoded_chunk
                entry_to_update["content"] = partial_text
        else:
            partial_text = clean_chunks(decoded_chunk)
            entry_to_update["content"] = partial_text

        saia_collection_v2.update(key=chat_key, chat_history=chat_history_obj)

        if decoded_chunk.startswith("### ERROR "):
            err_message = decoded_chunk.split("### ERROR ")[1]
            # SAIA-737: Streaming errors need to be handled at a higher level w/ our own error format
            # since requests library doesn't support HTTP trailers for this use case
            raise RuntimeError({"error": err_message})
        if not parser_validated and classification in [0, 3]: # Only validate for Write SPL and Optimize SPL
            spl, parser_validated, parser_error, warning_message = check_spl_with_parser_with_timeout(partial_text, tool_request, logger)
            if parser_error is not None:
                break

    # Before proceeding, check if stop state was set elsewhere
    # fetch chat history again
    ret = saia_collection_v2.get_chat(tool_request.chat_id)
    if ret is not None:
        (chat_key, chat_history_obj) = ret
        chat_history = chat_history_obj["records"]["write"]
        entry_to_update = chat_history[resp_index]
        if entry_to_update["loadingState"] == 4:
            logger.info("Generation was stopped, exiting early")
            return

    if not parser_validated and parser_error is not None and spl is not None and classification in [0, 3]: # Only validate for Write SPL and Optimize SPL:
        # Need to retry
        # Put original response in workflow block
        content = partial_text[0:partial_text.rfind("```")+3]
        draft_content_fn_open = f"<|start|>assistant<|channel|>commentary to=functions.draft_spl<|message|>{content}<|call|>\n"
        draft_content_fn_close = "<|start|>functions.draft_spl to=assistant<|channel|>commentary<|message|>Generated SPL was not parsable, invoking parser correction.<|end|>\n"

        if "reasoning" in entry_to_update:
            entry_to_update["reasoning"] = entry_to_update["reasoning"] + draft_content_fn_open + draft_content_fn_close
        else:
            entry_to_update["reasoning"] = draft_content_fn_open + draft_content_fn_close
        entry_to_update["content"] = ""
        saia_collection_v2.update(key=chat_key, chat_history=chat_history_obj)
        n = 0
        while n < 2 and not parser_validated and parser_error is not None and spl is not None:
            response = api.parser_retry(
                spl,
                parser_error.body.decode("utf-8"),
                job_id=tool_request.job_id,
                locale="en-US",
                log_to_telemetry=tool_request.logging_context["should_log_telemetry"],
                source_app_id=tool_request.logging_context["source_app_id"],
            )
            for text in response.iter_content(chunk_size=None):
                val = text.decode("utf-8", "ignore")  # pyright: ignore
                val = clean_chunks(val)
                partial_text = val
                entry_to_update["content"] = partial_text
                saia_collection_v2.update(key=chat_key, chat_history=chat_history_obj)
                if not parser_validated:
                    spl, parser_validated, parser_error, warning_message = check_spl_with_parser_with_timeout(partial_text, tool_request, logger)
                    if parser_error is not None:
                        break
            n += 1

    if not parser_validated and parser_error is not None:
        # Even after 2 retries, it failed, so surface a warning message that no SPL is valid
        logger.warning(log_kwargs(
            message="Warning: This SPL did not parse!",
            spl=spl,
            final_error=str(parser_error),
            **tool_request.logging_context,
        ))
        entry_to_update["metadata"]["warnings"].append(
            f"Sorry, this doesn't seem to be parsable SPL. Parsing error: {str(parser_error)}"
        )

    entry_to_update["loadingState"] = 2  # 0 - loading, 1 - streaming, 2 - complete, 3 - error, 4 - stopped
    entry_to_update["content"] = partial_text
    saia_collection_v2.update(key=chat_key, chat_history=chat_history_obj)
    end_time = time.time()
    logger.info(
        log_kwargs(
            apply_time=round((end_time - start_time), 5),
            **tool_request.logging_context,
        )
    )
    logger.info(
        log_kwargs(
            message="Finished write_spl tool invocation",
            **tool_request.logging_context,
        )
    )
    generate_chat_audit_log(
        tool_request.system_scoped_service,
        request_id=tool_request.job_id,
        chat_id=tool_request.chat_id,
        user=tool_request.user,
        role="assistant",
        content=partial_text,
    )
    generate_ai_service_usage_log(
        tool_request.system_scoped_service,
        request_id=tool_request.job_id,
        time=end_time,
        user=tool_request.user,
        prompt_types=[get_ai_usage_dashboard_string_for_classification(classification)],
        status=200,
        source_app_id=tool_request.logging_context["source_app_id"],
    )
