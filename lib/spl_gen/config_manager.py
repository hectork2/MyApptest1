import json

from splunklib.client import Service
from urllib.parse import urlencode

class ConfigManager:
    def __init__(self, service: Service, app: str, file_endpoint: str, owner: str = "nobody") -> None:
        self.service = service
        self.app = app
        self.file_endpoint = file_endpoint
        self.owner = owner

    def _decode_response_body(self, response):
        response_body = response["body"].read()
        return json.loads(response_body.decode("utf-8"))
    
    def _make_request(self, func, **kwargs):
        try:
            response = func(self.file_endpoint, owner=self.owner, app=self.app, output_mode="json", **kwargs)
            decoded_response_body = self._decode_response_body(response)
            return response["status"], decoded_response_body, decoded_response_body["entry"][0]["content"]
        except:
            return 500, {}, {}

    def fetch(self):
        return self._make_request(self.service.get)
    
    def update(self, body_params):
        return self._make_request(self.service.post, body=urlencode(body_params))
