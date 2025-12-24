# Copyright 2024 Splunk Inc.
import json
import sys
import uuid
import os
import copy
import time

from splunk.persistconn.application import PersistentServerConnectionApplication

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from base_rest import BaseRestUtils

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from spl_gen.saia_collection_v2 import SaiaCollectionV2
from spl_gen.saia_job_map_collection import SaiaJobMapCollection
from spl_gen.utils import get_classification_string_for_code, get_default_record_map, cleanup_chat_thread, log_kwargs, get_app_version, deterministic_hash
from spl_gen.remote.v1alpha1 import SaiaApi
class GenerationHandler(PersistentServerConnectionApplication, BaseRestUtils):

    def __init__(self, _command_line, _command_arg):
        super(PersistentServerConnectionApplication, self).__init__()
        super(BaseRestUtils, self).__init__()

    def handle(self, in_bytes):
        return self.handle_wrapper(in_bytes, self.handle_func)

    def handleStream(self, handle, in_bytes):
        """
        For future use
        """
        raise NotImplementedError("PersistentServerConnectionApplication.handleStream")

    def done(self):
        """
        Virtual method which can be optionally overridden to receive a
        callback after the request completes.
        """
        pass

    def handle_func(self, request):
        # Handle a syncronous from splunkd.
        service = self.service_from_request(request)
        system_scoped_service = self.service_from_request(request, use_system_token=True)
        app_version = get_app_version(system_scoped_service)
        session = request["session"]
        payload = self.get_payload(request)

        if (
            "prompt" in payload
            and "classification" in payload
            and "chat_id" in payload
        ):
            ns = request["ns"]
            user_prompt = payload["prompt"]
            if "lang" in request:
                locale = request["lang"]
            else:
                locale = "en-US"

            if "rewrite_content" in payload:
                rewrite_content = payload["rewrite_content"]
            else:
                rewrite_content = False

            chat_id = payload["chat_id"]
            user = session["user"]
            hashed_user = deterministic_hash(user)

            classification_string = get_classification_string_for_code(payload["classification"])
            # Check throttling state
            api = SaiaApi(service, system_scoped_service, user, chat_id, hashed_user)
            metering_request_id = str(uuid.uuid4())

            try:
                throttling_response = api.metering(metering_request_id)
                throttling_data = throttling_response.json()

                if classification_string in throttling_data:
                    if throttling_data[classification_string]["throttled"]:
                        return self.create_response({"throttling_data": throttling_data}, 429)
            except Exception as e:
                self.logger.error(f"Metering/throttling check failed: {str(e)}")

                # Extract error details from the exception
                error_message = "Unable to check metering and throttling status. Please try again later or contact support if the issue persists."
                error_status = 500

                try:
                    # The exception might contain a dictionary with status and error details
                    if hasattr(e, 'args') and len(e.args) > 0:
                        error_data = e.args[0]
                        if isinstance(error_data, dict):
                            if 'error' in error_data:
                                error_message = error_data['error']
                            if 'status' in error_data:
                                error_status = error_data['status']
                except Exception:
                    # If we can't parse the error, use the defaults
                    pass

                # Create error chat history entry so the user sees the error in the UI
                should_log_telemetry, enabled_features = self._fetch_telemetry_details(service, request, "generation")
                saia_collection_v2 = SaiaCollectionV2(service=service, username=session["user"])

                ret = saia_collection_v2.get_chat(payload["chat_id"])
                if ret is None:
                    # Need to create a new chat history for given chat_id
                    default_record_map = get_default_record_map()
                    saia_collection_v2.insert(chat_id, default_record_map)
                    chat_history_obj = default_record_map
                else:
                    (_, chat_history_obj) = ret

                chat_history_obj["assistantMode"] = classification_string
                thread_to_update = chat_history_obj["records"][classification_string]
                search_id = str(uuid.uuid4())

                # Add the incoming prompt to chat history
                thread_to_update.append(
                    {
                        "content": payload["prompt"],
                        "role": "user",
                        "id": search_id,
                        "chat_id": payload["chat_id"]
                    }
                )

                response_id = str(uuid.uuid4())
                # Add the error response to the chat history
                error_entry = {
                    "prompt": payload["prompt"],
                    "content": error_message,
                    "role": "assistant",
                    "id": response_id,
                    "chat_id": payload["chat_id"],
                    "loadingState": 3,  # 3 = error state
                    "enabledFeatures": enabled_features,
                    "metadata": {
                        "error": error_message,
                        "status": error_status
                    }
                }

                thread_to_update.append(error_entry)
                chat_history_obj["lastModified"] = time.time() * 1000
                chat_history_obj["empty"] = False

                # Store the error chat history
                saia_collection_v2.update(chat_id, chat_history_obj)

                # Return error response with response_id so frontend can track it
                return self.create_response({
                    "error": error_message,
                    "response_id": response_id,
                    "job_id": response_id,
                    "status": error_status
                }, error_status)

            should_log_telemetry, enabled_features = self._fetch_telemetry_details(service, request, "generation")

            chat_id = payload["chat_id"]
            user = session["user"]
            hashed_user = deterministic_hash(user)

            source_app_id = request["header_map"][self.SOURCE_APP_ID_KEY]

            # Update chat history with user prompt and loading response
            saia_collection_v2 = SaiaCollectionV2(service=service, username=session["user"])
            ret = saia_collection_v2.get_chat(payload["chat_id"])
            if ret is None:
                # Need to create a new chat history for given chat_id
                default_record_map = get_default_record_map()
                saia_collection_v2.insert(chat_id, default_record_map)
                chat_history_obj = default_record_map
            else:
                (_, chat_history_obj) = ret

            chat_history_obj["assistantMode"] = classification_string
            thread_to_update = chat_history_obj["records"][classification_string]
            was_chat_empty = chat_history_obj["empty"]
            search_id = str(uuid.uuid4())

            # Add the incoming prompt to chat history
            thread_to_update.append(
                {
                    "content": payload["prompt"],
                    "role": "user",
                    "id": search_id,
                    "chat_id": payload["chat_id"]
                }
            )
            response_id = str(uuid.uuid4())
            # Add the response placeholder to the chat history
            chat_history_with_response = copy.deepcopy(chat_history_obj)

            entry_to_update = {
                "prompt": payload["prompt"],
                "content": "",
                "role": "assistant",
                "id": response_id,
                "chat_id": payload["chat_id"],
                "loadingState": 0,  # 0 - loading, 1 - streaming, 2 - complete, 3 - error, 4 - tool use
                "enabledFeatures": enabled_features
            }

            chat_history_with_response["lastModified"] = time.time() * 1000
            chat_history_with_response["empty"] = False

            tool_id = None
            # Special case, loading state of tool use case will be 4 - tool use, as state is handled by the tool component itself
            if payload["classification"] == 0:
                self.logger.info(
                    log_kwargs(
                        message="Invoking write_spl tool.",
                        request_id=response_id,
                        chat_id=payload["chat_id"],
                        user=hashed_user,
                        source_app=source_app_id,
                        saia_app_version=app_version,
                    )
                )
                entry_to_update["toolId"] = "write_spl"
                tool_id = "write_spl"

            chat_history_with_response["records"][classification_string].append(
                entry_to_update
            )
            # Write chat history with response placeholder to KV
            saia_collection_v2.update(chat_id, chat_history_with_response)

            # We pass the response history without the placeholder to SCS
            source_app_id = request["header_map"][self.SOURCE_APP_ID_KEY]

            job_map = SaiaJobMapCollection(system_scoped_service)

            async_job_payload = {
                "app_name": ns["app"],
                "user": user,
                "token": session["authtoken"],
                "hashed_user": hashed_user,
                "user_prompt": user_prompt,
                "chat_history": json.dumps(cleanup_chat_thread(thread_to_update)),
                "classification": payload["classification"],
                "locale": locale,
                "log_to_telemetry": should_log_telemetry,
                "was_chat_empty": was_chat_empty,
                "source_app_id": source_app_id,
                "rewrite_content": rewrite_content,
                "chat_id": chat_id,
                "response_id": response_id,
                "app_version": app_version,
                "source_app_id": source_app_id,
                "tool_id": tool_id
            }

            job_map.insert_job(
                job_id=response_id,
                job_payload=async_job_payload
            )
            self.logger.info(
                log_kwargs(
                    message="Inserted generation job to map.",
                    request_id=response_id,
                    chat_id=chat_id,
                    user=hashed_user,
                    source_app=source_app_id,
                    saia_app_version=app_version,
                )
            )
            return self.create_response({"job_id": response_id, "response_id": response_id, "throttling_data": throttling_data}, 200)

        return self.create_response({"error": "Incorrect arguments provided"}, 400)
