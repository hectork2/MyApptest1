import logging
import time
import json
class SaiaToolScratchpad:
    def __init__(self, service, username):
        try:
            self.logger = logging.Logger(self.__class__.__name__)
            self.saia_tool_scratchpad_data = service.kvstore["saia_tool_scratchpad"]
            self.username = username
        except KeyError:
            raise Exception("KVStore collection not found")

    def clean(self, id):
        self.saia_tool_scratchpad_data.data.delete_by_id(
            id
        )
        # TODO: Delete all tool scratchpad entries that belong to the current user, which are marked as "finished", with time finished over 1 day ago
        # self.saia_tool_scratchpad_data.data.delete(
        #     query=json.dumps({"saia_user": self.username, "state": "finished"})
        # )

    def get(self, id):
        result = self.saia_tool_scratchpad_data.data.query_by_id(id)
        return result

    def insert(self, id, content):
        return self.saia_tool_scratchpad_data.data.insert(
            {"_key": id, "saia_user": self.username, "content": content, "state": "new", "last_updated": time.time()}
        )

    def update(self, id, content, state):
        return self.saia_tool_scratchpad_data.data.update(
            id, {"saia_user": self.username, "content": content, "state": state, "last_updated": time.time()}
        )

    def delete(self, id):
        return self.saia_tool_scratchpad_data.data.delete_by_id(
            id
        )
