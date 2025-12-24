import uuid
import json
import time
import logging
import re
from requests import HTTPError
from threading import Thread
from splunklib.binding import HTTPError as SplunkHTTPError
from ..utils import clean_chunks, log_kwargs, get_classification_string_for_code, get_telemetry_string_for_classification, get_ai_usage_dashboard_string_for_classification, get_allowable_ast_parsing_warning, unwrap_runtime_error, SimpleError
from ..utils.audit_logging import generate_chat_audit_log, generate_ai_service_usage_log
from .v1alpha1 import SaiaApi
from ..tools.constants import TOOL_MAPPING
from ..tools import ToolRequest
from ..saia_collection_v2 import SaiaCollectionV2
from ..saia_job_map_collection import SaiaJobMapCollection


class AsyncHttpJobs:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def create_from_request(self, app_name, user, hashed_user, kwargs, service, system_scoped_service, chat_id, response_id, app_version, source_app_id, should_log_telemetry, tool_id=None):
        job_id = response_id # job_id (cross-env telemetry tracking ID) also maps to ID of response chat object
        kwargs = {
            "job_id": job_id,
            "user": user,
            "hashed_user": hashed_user,
            "kwargs": kwargs,
            "service": service,
            "system_scoped_service": system_scoped_service,
            "chat_id": chat_id,
            "app_version": app_version,
            "source_app_id": source_app_id,
            "should_log_telemetry": should_log_telemetry,
            "tool_id": tool_id,
        }

        self.logger.info(
            log_kwargs(
                message=f"starting job {job_id}",
                request_id=job_id,
                chat_id=chat_id,
                tool_id=tool_id,
                user=hashed_user,
                saia_app_version=app_version,
            )
        )

        if tool_id is not None:
            # Tool calling flow
            thread = Thread(target=self.make_tool_request, kwargs=kwargs)
        else:
            thread = Thread(target=self.make_search_request, kwargs=kwargs)
        thread.start()
        return job_id

    def make_search_request(self, job_id, user, hashed_user, kwargs, service, system_scoped_service, chat_id, app_version, source_app_id, should_log_telemetry, tool_id):
        classification = get_classification_string_for_code(kwargs["classification"])

        rewrite_content = kwargs["rewrite_content"]

        logging_context = dict(
            request_id=job_id,
            chat_id=chat_id,
            user=hashed_user,
            saia_app_version=app_version,
            source_app_id=source_app_id,
            classification=get_telemetry_string_for_classification(kwargs["classification"]),
        )

        start_time = time.time()
        self.logger.info(
            log_kwargs(message="Starting generate.", **logging_context)
        )
        generate_chat_audit_log(
            system_scoped_service,
            request_id=job_id,
            chat_id=chat_id,
            user=user,
            role="user",
            content=kwargs["user_prompt"],
        )
        api = SaiaApi(service, system_scoped_service, user, chat_id, hashed_user)
        saia_collection_v2 = SaiaCollectionV2(service, username=user)
        saia_job_map_collection = SaiaJobMapCollection(system_scoped_service)

        ret = saia_collection_v2.get_chat(chat_id)
        if ret is not None:
            (key, chat_history_obj) = ret
        else:
            # We should not get here!
            self.logger.warning(
                log_kwargs(message="Chat ID not found. This should not happen!", **logging_context)
            )
            return
        metadata = {"warnings": []}

        records = chat_history_obj["records"][classification]
        resp_index = next((i for i, item in enumerate(records) if item["id"] == job_id), -1)
        if resp_index == -1:
            self.logger.info(
                log_kwargs(message="Response ID not found. This should not happen!", **logging_context)
            )
            raise Exception("Response ID not found")
        entry_to_update = records[resp_index]

        kwargs["use_state_streamer"] = False
        try:
            kwargs["job_id"] = job_id
            kwargs["indexed_fields"] = []
            kwargs["ast"] = ""

            kwargs["rag_data_only"] = False
            # Special case: For Explain SPL skill, need to run the AST parser
            if classification == "explain":
                try:
                    user_prompt = kwargs["user_prompt"].strip()
                    prompt_to_parse = user_prompt
                    if not user_prompt.startswith("|") and not user_prompt.startswith(
                        "search"
                    ):
                        prompt_to_parse = "search " + user_prompt
                    # A nasty bit of code to get the ast string in a deserializable format that SAIA API understands
                    ast = json.dumps(
                        json.loads(
                            service.parse(query=prompt_to_parse, output_mode="json")[
                                "body"
                            ].read()
                        )
                    )
                    kwargs["ast"] = ast
                except SplunkHTTPError as e:
                    if e.status == 400:
                        # Allow queries with missing macros, lookups, or data models to still be explained
                        if (
                            e.body
                        ):
                            allowable_ast_parsing_warning = get_allowable_ast_parsing_warning(e.body.decode("utf-8"))
                            if not allowable_ast_parsing_warning:
                                self.logger.info(
                                    log_kwargs(
                                        message=f"Encountered error body {str(e.body)} when running SPL through AST parser",
                                        **logging_context,
                                    )
                                )
                                error_append = ""
                                try:
                                    error_body = json.loads(e.body)
                                    error_append = (
                                        f" Parsing error: {error_body['messages'][0]['text']}"
                                    )
                                except Exception as e:
                                    self.logger.info(
                                        log_kwargs(
                                            message=f"Encountered error {str(e)} when parsing error body",
                                            **logging_context,
                                        )
                                    )
                                raise SimpleError(
                                    f"Sorry, this doesn't seem to be parsable SPL.{error_append}"
                                )
                            else:
                                metadata["warnings"].append(allowable_ast_parsing_warning)
                    else:
                        raise
            response = api.search(**kwargs)
            for key, value in response.headers.items():
                if key.lower().startswith("warning-"):
                    metadata["warnings"].append(value)
                if key.lower() == "metadata-source-urls":
                    metadata["sourceUrls"] = json.loads(value)
                if key.lower() == "metadata-source-titles":
                    metadata["sourceTitles"] = json.loads(value)
                if key.lower() == "metadata-chat-title-summary":
                    # For previously new/empty chats, update thread with chat title summary
                    # Headers are decoded assuming latin-1 by default, but we need to use UTF-8 to support multiple languages
                    chat_history_obj["threadName"] = value.encode('latin1').decode('unicode-escape')
                    saia_collection_v2.update(key=chat_id, chat_history=chat_history_obj)
                if key.lower() == "notify-reload-required":
                    metadata["reloadRequired"] = value

            entry_to_update["metadata"] = metadata
            partial_text = ""
            entry_to_update["loadingState"] = 1  # 0 - loading, 1 - streaming, 2 - complete, 3 - error, 4 - stopped
            if rewrite_content:
                entry_to_update["rewriteContent"] = rewrite_content

            first_token_received = False
            for text in response.iter_content(chunk_size=None):
                if not first_token_received:
                    first_token_received = True
                    first_token_time = time.time()
                    self.logger.info(
                        log_kwargs(
                            time_to_first_token=round((first_token_time - start_time), 5),
                            **logging_context,
                        )
                    )
                decoded_chunk = text.decode('utf-8', 'ignore')  # pyright: ignore
                decoded_chunk = clean_chunks(decoded_chunk)
                if decoded_chunk.startswith("### ERROR "):
                    err_message = decoded_chunk.split("### ERROR ")[1]
                    # SAIA-737: Streaming errors need to be handled at a higher level w/ our own error format
                    # since requests library doesn't support HTTP trailers for this use case
                    raise RuntimeError({"error": err_message})
                if rewrite_content:
                    partial_text = decoded_chunk
                else:
                    partial_text += decoded_chunk
                entry_to_update["content"] = partial_text
                saia_collection_v2.update(key=chat_id, chat_history=chat_history_obj)


            # Before proceeding, check if stop state was set elsewhere
            # fetch chat history again
            ret = saia_collection_v2.get_chat(chat_id)
            if ret is not None:
                (chat_key, chat_history_obj) = ret
                chat_history = chat_history_obj["records"][classification]
                entry_to_update = chat_history[resp_index]
                if entry_to_update["loadingState"] == 4:
                    self.logger.info("Generation was stopped, exiting early")
                    return

            entry_to_update["loadingState"] = 2  # 0 - loading, 1 - streaming, 2 - complete, 3 - error, 4 - stopped
            entry_to_update["content"] = partial_text
            saia_collection_v2.update(key=chat_id, chat_history=chat_history_obj)
            self.logger.info(
                log_kwargs(
                    message=f"Deleting job id from job map {job_id}.",
                    **logging_context,
                )
            )
            try:
                saia_job_map_collection.delete_job(job_id)
            except Exception as e:
                self.logger.info(
                    log_kwargs(
                        message=f"Error deleting job id {job_id} from job map: {repr(e)}",
                        **logging_context,
                    )
                )
            end_time = time.time()
            self.logger.info(
                log_kwargs(
                    apply_time=round((end_time - start_time), 5),
                    **logging_context,
                )
            )
            self.logger.info(
                log_kwargs(
                    message="Generation complete.",
                    **logging_context,
                )
            )
            generate_chat_audit_log(
                system_scoped_service,
                request_id=job_id,
                chat_id=chat_id,
                user=user,
                role="assistant",
                content=partial_text,
            )
            generate_ai_service_usage_log(
                system_scoped_service,
                request_id=job_id,
                time=end_time,
                user=user,
                prompt_types=[get_ai_usage_dashboard_string_for_classification(kwargs["classification"])],
                status=200,
                source_app_id=source_app_id
            )
        except Exception as e:
            end_time = time.time()
            status = 500 # Default is internal server error
            if (
                isinstance(e, HTTPError)
                and "detail" in e.response.json()
            ):
                logging.exception(f"Received an error with status code {e.response.status_code}")
                metadata["error"] = str(e.response.json()["detail"])
                entry_to_update["loadingState"] = 3  # 0 - loading, 1 - streaming, 2 - complete, 3 - error, 4 - stopped
                entry_to_update["metadata"] = metadata
                saia_collection_v2.update(key=chat_id, chat_history=chat_history_obj)
                status = e.response.status_code

            elif (
                isinstance(e, RuntimeError)
            ):
                logging.exception(f"Received a runtime error {repr(e)}")
                error_content = unwrap_runtime_error(e)
                metadata["error"] = error_content
                entry_to_update["loadingState"] = 3  # 0 - loading, 1 - streaming, 2 - complete, 3 - error, 4 - stopped
                entry_to_update["metadata"] = metadata
                saia_collection_v2.update(key=chat_id, chat_history=chat_history_obj)

            else:
                logging.exception(f"unknown exception {repr(e)}")
                metadata["error"] = str(e)
                entry_to_update["loadingState"] = 3  # 0 - loading, 1 - streaming, 2 - complete, 3 - error, 4 - stopped
                entry_to_update["metadata"] = metadata
                saia_collection_v2.update(key=chat_id, chat_history=chat_history_obj)
            generate_ai_service_usage_log(
                system_scoped_service,
                request_id=job_id,
                time=end_time,
                user=user,
                prompt_types=[get_ai_usage_dashboard_string_for_classification(kwargs["classification"])],
                status=status,
                source_app_id=source_app_id
            )

    def make_tool_request(self, job_id, user, hashed_user, kwargs, service, system_scoped_service, chat_id, app_version, source_app_id, should_log_telemetry, tool_id):
        try:
            logging_context = dict(
                request_id=job_id,
                chat_id=chat_id,
                user=hashed_user,
                saia_app_version=app_version,
                source_app_id=source_app_id,
                tool_id=tool_id,
                should_log_telemetry=should_log_telemetry,
            )

            self.logger.info(
                log_kwargs(
                    message="Starting tool call",
                    **logging_context
                )
            )
            tool_request = ToolRequest(
                tool_id,
                job_id,
                chat_id,
                user,
                kwargs,
                service,
                system_scoped_service,
                logging_context,
            )

            tool_request.tool_id = tool_id
            if tool_id in TOOL_MAPPING:
                self.logger.info(
                    log_kwargs(
                        message="Invoking tool function",
                        **logging_context
                    )
                )
                # Invoke tool function
                TOOL_MAPPING[tool_id](tool_request)
            else:
                self.logger.info(
                    log_kwargs(
                        message="Warning: Tool function not found!",
                        **logging_context
                    )
                )
        except Exception as e:
            self.logger.info(
                log_kwargs(
                    message="Encountered exception when running.",
                    exception=e
                )
            )
