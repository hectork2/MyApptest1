import json
import uuid
import logging

from .utils import get_default_record_map, get_chat_history_obj

saia_collection_v2_kv_lock = None
try:
    from .utils.rest_server.lock import GroupLock, KvObjectLocker
    saia_collection_v2_kv_lock = KvObjectLocker(fail_without_lock=True, use_file_locks=True, file_namespace="saia_collection_v2_file_namespace", wait_time=1)
except:
    logging.warning("Warning: Could not import splunk python package (requires Splunk be installed)")

class SaiaCollectionV2:
    def __init__(self, service, username):
        try:
            self.logger = logging.Logger(self.__class__.__name__)
            self.saia_collection_data = service.kvstore["saia_collection_v2"]
            self.username = username
        except KeyError:
            raise Exception("KVStore collection not found")

    def get_all_chats(self):
        results = self.saia_collection_data.data.query(
            query={"saia_user": self.username}
        )
        ret = []
        for result in results:
            ret.append((result["_key"], get_chat_history_obj(result)))
        return ret

    def get_chat(self, thread_id):
        try:
            result = self.saia_collection_data.data.query_by_id(thread_id)
            return (result["_key"], get_chat_history_obj(result))
        except Exception as e:
            logging.info(f"Caught error {e}")
            return None

    def insert(self, chat_id, chat_history):
        if saia_collection_v2_kv_lock is not None:
            with GroupLock([self.username], saia_collection_v2_kv_lock): # type: ignore
                return self.saia_collection_data.data.insert(
                    {"_key": chat_id, "saia_user": self.username, "chat_history": chat_history}
                )
        else:
            self.logger.warning("Warning: Could not obtain lock, writing dangerously")
            return self.saia_collection_data.data.insert(
                {"_key": chat_id, "saia_user": self.username, "chat_history": chat_history}
            )

    def delete(self, chat_id):
        if saia_collection_v2_kv_lock is not None:
            with GroupLock([self.username], saia_collection_v2_kv_lock): # type: ignore
                return self.saia_collection_data.data.delete_by_id(chat_id)
        else:
            self.logger.warning("Warning: Could not obtain lock, deleting dangerously")
            return self.saia_collection_data.data.delete_by_id(chat_id)

    def merge_chat_with_existing(self, chat_id, chat_history):
        result = self.saia_collection_data.data.query_by_id(chat_id)
        current_chat_history = get_chat_history_obj(result)
        # merge the new chat history with the existing one, retaining the stopped state
        map = {}
        for thread in current_chat_history["records"].values():
            for entry in thread:
                map[entry["id"]] = entry
        for thread in chat_history["records"].values():
            for entry in thread:
                if entry["id"] in map and "stopped" in map[entry["id"]]:
                    entry["stopped"] = map[entry["id"]]["stopped"]

    def update(self, key, chat_history):
        if saia_collection_v2_kv_lock is not None:
            with GroupLock([self.username], saia_collection_v2_kv_lock): # type: ignore
                self.merge_chat_with_existing(key, chat_history)
                return self.saia_collection_data.data.update(
                    key, {"saia_user": self.username, "chat_history": chat_history}
                )
        else:
            self.logger.warning("Warning: Could not obtain lock, writing dangerously")
            self.merge_chat_with_existing(key, chat_history)
            return self.saia_collection_data.data.update(
                key, {"saia_user": self.username, "chat_history": chat_history}
            )

    def clear(self):
        if saia_collection_v2_kv_lock is not None:
            with GroupLock([self.username], saia_collection_v2_kv_lock): # type: ignore
                return self.saia_collection_data.data.delete(
                    query=json.dumps({"saia_user": self.username})
                )
        else:
            self.logger.warning("Warning: Could not obtain lock, deleting dangerously")
            return self.saia_collection_data.data.delete(
                query=json.dumps({"saia_user": self.username})
            )
