import json
import logging

from .utils import get_default_record_map, get_chat_history_obj

saia_collection_kv_lock = None
try:
    from .utils.rest_server.lock import GroupLock, KvObjectLocker
    saia_collection_kv_lock = KvObjectLocker(fail_without_lock=True, use_file_locks=True, file_namespace="saia_collection_file_namespace", wait_time=1)
except:
    logging.warning("Warning: Could not import splunk python package (requires Splunk be installed)")

class SaiaCollection:
    def __init__(self, service, username):
        try:
            self.logger = logging.Logger(self.__class__.__name__)
            self.saia_collection_data = service.kvstore["saia_collection"]
            self.username = username
        except KeyError:
            raise Exception("KVStore collection not found")

    def get(self):
        results = self.saia_collection_data.data.query(
            query={"saia_user": self.username}
        )
        if len(results) == 0:
            return None
        else:
            return (results[0]["_key"], get_chat_history_obj(results[0]))

    def insert(self, chat_history):
        if saia_collection_kv_lock is not None:
            with GroupLock([self.username], saia_collection_kv_lock): # type: ignore
                return self.saia_collection_data.data.insert(
                    {"saia_user": self.username, "chat_history": chat_history}
                )
        else:
            self.logger.warning("Warning: Could not obtain lock, writing dangerously")
            return self.saia_collection_data.data.insert(
                    {"saia_user": self.username, "chat_history": chat_history}
                )
            

    def update(self, key, chat_history):
        if saia_collection_kv_lock is not None:
            with GroupLock([self.username], saia_collection_kv_lock): # type: ignore
                return self.saia_collection_data.data.update(
                    key, {"saia_user": self.username, "chat_history": chat_history}
                )
        else:
            self.logger.warning("Warning: Could not obtain lock, writing dangerously")
            return self.saia_collection_data.data.update(
                    key, {"saia_user": self.username, "chat_history": chat_history}
                )

    def clear(self):
        return self.saia_collection_data.data.delete(
            query=json.dumps({"saia_user": self.username})
        )
