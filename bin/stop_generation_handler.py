import os, sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from base_rest import BaseRestUtils
from splunk.persistconn.application import PersistentServerConnectionApplication


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from spl_gen.remote.v1alpha1 import SaiaApi
from spl_gen.utils import log_kwargs, deterministic_hash
from spl_gen.saia_collection_v2 import SaiaCollectionV2

class StopGenerationHandler(PersistentServerConnectionApplication, BaseRestUtils):

    def __init__(self, _command_line, _command_arg):
        super(PersistentServerConnectionApplication, self).__init__()
        super(BaseRestUtils, self).__init__()

    def handle(self, in_bytes):
        return self.handle_wrapper(in_bytes, self.handle_func)

    def done(self):
        """
        Virtual method which can be optionally overridden to receive a
        callback after the request completes.
        """
        pass

    def handle_func(self, request):
        self.logger.info("Stop generation handler called")
        service = self.service_from_request(request)
        system_scoped_service = self.service_from_request(request, use_system_token=True)
        session = request["session"]
        user = session["user"]
        hashed_user = deterministic_hash(user)
        api = SaiaApi(service, system_scoped_service, user, "metering_handler", hashed_user)
        payload = self.get_payload(request)
        if "job_id" in payload and "chat_id" in payload:
            self.logger.info(f"Stopping generation for {payload['job_id']}")
            api.stop_generation(payload["job_id"])

            # Mark chat entry as stopped
            saia_collection_v2 = SaiaCollectionV2(service, user)
            chat_id = payload["chat_id"]
            ret = saia_collection_v2.get_chat(chat_id)
            if ret is not None:
                (chat_key, chat_history_obj) = ret
                found = False
                for _, thread in chat_history_obj["records"].items():
                    if found:
                        break
                    for entry in thread:
                        if entry["id"] == payload["job_id"]:
                            entry["loadingState"] = 4
                            entry["stopped"] = True
                            found = True
                            break
                saia_collection_v2.update(chat_key, chat_history_obj)
            else:
                self.logger.warning(f"Chat ID {chat_id} not found. Cannot update status.")

            self.logger.info(f"Stopped generation for {payload['job_id']}")
            return {"payload": "OK", "status": 200}

        return {"status": "Missing job_id", "status": 400}
