import uuid
import sys
import os
import json
from splunk.persistconn.application import PersistentServerConnectionApplication

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from base_rest import BaseRestUtils

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from spl_gen.utils import log_kwargs, cleanup_chat_history, get_app_version, get_default_record_map, get_classification_string_for_code
from spl_gen.saia_collection import SaiaCollection
from spl_gen.saia_collection_v2 import SaiaCollectionV2

ERROR_INVALID_PATH = "Path does not exist"
ERROR_INVALID_CHAT_ID = "Provided chat_id does not exist"


class ChatHistoryHandler(PersistentServerConnectionApplication, BaseRestUtils):

    def __init__(self, _command_line, _command_arg):
        super(PersistentServerConnectionApplication, self).__init__()
        super(BaseRestUtils, self).__init__()

    def handle(self, in_bytes):
        # return {"payload": "hello", "status": 200}
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

    def fetch_chats(self, saia_collection, saia_collection_v2, should_clean, logging_context):
        # Fetch all chats
        records = saia_collection_v2.get_all_chats()
        if len(records) == 0:
            self.logger.info(
                log_kwargs(
                    message="Found no entries in V2 chat history",
                    **logging_context,
                )
            )
            # Try migrating V1 saia_collection
            ret = saia_collection.get()
            if ret is not None:
                self.logger.info(
                    log_kwargs(
                        message="Migrating V1 chat history to V2.",
                        **logging_context,
                    )
                )
                # We have a V1 saia_collection chat history which needs migration
                (_, chat_history_obj) = ret
                for key, value in chat_history_obj.items():
                    saia_collection_v2.insert(key, value)
                self.logger.info(
                    log_kwargs(
                        message="Migrated V1 chat history to V2.",
                        **logging_context,
                    )
                )
            else:
                # We do not have a V1 saia_collection, no need to migrate
                # But we do need to insert a default empty collection
                self.logger.info(
                    log_kwargs(
                        message="Found no V1 chat history, creating default V2 chat thread.",
                        **logging_context,
                    )
                )
                chat_id = str(uuid.uuid4())
                default_record_map = get_default_record_map()
                saia_collection_v2.insert(chat_id, default_record_map)
                chat_history_obj = {
                    chat_id: default_record_map
                }
        else:
            self.logger.info(
                log_kwargs(
                    message="Fetched V2 chat history.",
                    **logging_context,
                )
            )
            chat_history_obj = {}
            for record in records:
                chat_history_obj[record[0]] = record[1]
        if should_clean:
            chat_history_obj = cleanup_chat_history(chat_history_obj)
            for key, value in chat_history_obj.items():
                saia_collection_v2.update(key, value)
            self.logger.info(
                log_kwargs(
                    message="Cleaned V2 chat history.",
                    **logging_context,
                )
            )
        return chat_history_obj

    def handle_get_request(self, query_params, path, saia_collection, saia_collection_v2, logging_context):
        path_components = path.strip("/").split("/")
        chat_id = None
        if len(path_components) == 2:
            # chat_id is specified as path variable
            chat_id = path_components[1]
        elif len(path_components) > 2:
            return self.create_response({"error": ERROR_INVALID_PATH}, 404)

        logging_context["chat_id"] = chat_id

        include_records = query_params.get("include_records", True)
        should_clean = query_params.get("should_clean", False)

        self.logger.info(
            log_kwargs(
                message="Fetching chat history.",
                **logging_context,
            )
        )
        if chat_id is not None:
            # Fetch one chat by chat_id
            self.logger.info(
                log_kwargs(
                    message="Fetching one chat.",
                    **logging_context,
                )
            )
            ret = saia_collection_v2.get_chat(chat_id)
            if ret is not None:
                (chat_id, chat_history_obj) = ret
            else:
                return self.create_response({"error": ERROR_INVALID_CHAT_ID}, 404)
        else:
            chat_history_obj = self.fetch_chats(saia_collection, saia_collection_v2, should_clean, logging_context)

        if not include_records:
            # Remove records from chat entries
            if "records" in chat_history_obj:
                del chat_history_obj["records"]
            else:
                for thread in chat_history_obj.values():
                    del thread["records"]

        results = {"chat_history": chat_history_obj}
        return self.create_response(json.dumps(results), 200)

    def handle_post_request(self, payload, saia_collection, saia_collection_v2, logging_context):
        if "chat_id" in payload and "new_title" in payload:
            # If payload has been provided with chat_id and title, perform rename operation
            self.logger.info(
                log_kwargs(
                    chat_id=payload["chat_id"],
                    new_title=payload["new_title"],
                    message="Renaming chat.",
                    **logging_context,
                )
            )

            ret = saia_collection_v2.get_chat(payload["chat_id"])
            if ret is not None:
                (_, chat_history_obj) = ret
            else:
                return self.create_response({"error": ERROR_INVALID_CHAT_ID}, 404)

            chat_history_obj["threadName"] = payload["new_title"]
            saia_collection_v2.update(payload["chat_id"], chat_history_obj)
            results = {"chat_history": chat_history_obj}
        elif "chat_id" in payload and "classification" in payload and "chat_entry_id" in payload and "content" in payload and "tool_data" in payload:
            self.logger.info(log_kwargs(
                    chat_id=payload["chat_id"],
                    chat_entry_id=payload["chat_entry_id"],
                    message="Updating chat entry with new content.",
                    **logging_context,
                )
            )
            classification = get_classification_string_for_code(payload["classification"])
            # If chat_id, chat_entry_id and content are provided, perform an update to that entry
            if payload["chat_id"] in chat_history_obj:
                records = chat_history_obj[payload["chat_id"]]["records"][classification]
                resp_index = next((i for i, item in enumerate(records) if item["id"] == payload["chat_entry_id"]), -1)
                if resp_index == -1:
                    self.logger.info(
                        log_kwargs(message="Response ID not found. This should not happen!", **logging_context)
                    )
                    raise Exception("Response ID not found")
                entry_to_update = records[resp_index]
                entry_to_update["loadingState"] = 2
                entry_to_update["content"] = payload["content"]
                entry_to_update["tool_data"] = payload["tool_data"]
                saia_collection_v2.update(key=payload["chat_id"], chat_history=chat_history_obj)
            results = {"chat_history": chat_history_obj}
        else:
            self.logger.info(
                log_kwargs(
                    chat_id=None,
                    message="Fetching chat history via POST request.",
                    **logging_context,
                )
            )

            # Fetches chat history, with side effect of cleaning it up
            chat_history_obj = self.fetch_chats(
                saia_collection,
                saia_collection_v2,
                True,
                logging_context
            )
            results = {"chat_history": chat_history_obj}

        return self.create_response(json.dumps(results), 200)

    def handle_delete_request(self, payload, saia_collection_v2, logging_context):
        if "chat_id" in payload:
            self.logger.info(
                log_kwargs(
                    chat_id=payload["chat_id"],
                    message="Handling DELETE request to delete chat thread",
                    **logging_context,
                )
            )
            saia_collection_v2.delete(payload["chat_id"])
            records = saia_collection_v2.get_all_chats()
            chat_history_obj = {}
            for record in records:
                chat_history_obj[record[0]] = record[1]
            results = {"chat_history": chat_history_obj}
        else:
            self.logger.info(
                log_kwargs(
                    chat_id=None,
                    message="Handling DELETE request to delete chat history",
                    **logging_context,
                )
            )
            results = []
            saia_collection_v2.clear()

        return self.create_response(json.dumps(results), 204)

    def handle_func(self, request):
        # Handle a syncronous from splunkd.
        service = self.service_from_request(request)
        system_scoped_service = self.service_from_request(request, use_system_token=True)
        logging_uuid = str(uuid.uuid4())
        hashed_user = hash(request["session"]["user"])
        app_version = get_app_version(system_scoped_service)
        source_app_id = request["header_map"][self.SOURCE_APP_ID_KEY]

        logging_context = dict(
            request_id=logging_uuid,
            user=hashed_user,
            source_app=source_app_id,
            saia_app_version=app_version
        )

        self.logger.info(
            log_kwargs(
                message="Handling chat history request",
                **logging_context,
            )
        )

        saia_collection = SaiaCollection(
            service, request["session"]["user"] # pyright: ignore
        )
        saia_collection_v2 = SaiaCollectionV2(
            service, request["session"]["user"]
        )
        query_params = self.get_query_params(request)
        payload = self.get_payload(request)
        path = request.get("rest_path")

        if request["method"] == "GET":
            return self.handle_get_request(query_params, path, saia_collection, saia_collection_v2, logging_context)
        elif request["method"] == "POST":
            return self.handle_post_request(payload, saia_collection, saia_collection_v2, logging_context)
        elif request["method"] == "DELETE":
            return self.handle_delete_request(payload, saia_collection_v2, logging_context)

        # TODO: this needs to be more accurate
        return self.create_response({"error": "Something went wrong"}, 500)
