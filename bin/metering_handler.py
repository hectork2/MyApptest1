import os, sys, uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from base_rest import BaseRestUtils
from splunk.persistconn.application import PersistentServerConnectionApplication


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from spl_gen.remote.v1alpha1 import SaiaApi
from spl_gen.utils import deterministic_hash

class VersionHandler(PersistentServerConnectionApplication, BaseRestUtils):

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
        service = self.service_from_request(request)
        system_scoped_service = self.service_from_request(request, use_system_token=True)
        session = request["session"]
        user = session["user"]
        hashed_user = deterministic_hash(user)
        api = SaiaApi(service, system_scoped_service, user, "metering_handler", hashed_user)
        request_id = str(uuid.uuid4())
        metering_response = api.metering(request_id)
        return {"payload": metering_response.json(), "status": 200}
