import os, sys
import json
import uuid

from splunk.persistconn.application import PersistentServerConnectionApplication

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from base_rest import BaseRestUtils

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from spl_gen.remote.v1alpha1 import SaiaApi
from spl_gen.utils import deterministic_hash

import logging

class PersonalizedMetadataHandler(PersistentServerConnectionApplication, BaseRestUtils):
	def __init__(self, _command_line, _command_arg):
		super(PersistentServerConnectionApplication, self).__init__()
		super(BaseRestUtils, self).__init__()
		self.logger = logging.getLogger(self.__class__.__name__)

	def handle(self, in_bytes):
		return self.handle_wrapper(in_bytes, self.handle_func)

	def done(self):
		"""
		Virtual method which can be optionally overridden to receive a
		callback after the request completes.
		"""
		pass

	def handle_search_request(
		self,
		saia_api,
		query_params,
		job_id,
		locale,
		should_log_telemetry,
		source_app_id,
	):
		"""
		Handle search call for personalized metadata.
		"""
		user_prompt = query_params["prompt"]

		# Make request directly to SAIA SCS API, for personalizedmetadata
		chat_history = [
			{
				"content": user_prompt,
				"role": "user",
				"id": 0
			}
		]

		result = saia_api.search(
			job_id=job_id,
			user_prompt=user_prompt,
			chat_history=json.dumps(chat_history),
			classification=0,
			locale=locale,
			log_to_telemetry=should_log_telemetry,
			was_chat_empty=False,
			source_app_id=source_app_id,
			ast="",
			rag_data_only=True,
			rewrite_content=False,
			indexed_fields=[],
			use_state_streamer=False,
        )

		return {"payload": result.text, "status": 200}

	def handle_func(self, request):
		"""
		Handle a syncronous from splunkd.
		"""
		service = self.service_from_request(request)
		system_scoped_service = self.service_from_request(request, use_system_token=True)
		session = request["session"]
		user = session["user"]
		hashed_user = deterministic_hash(user)
		query_params = self.get_query_params(request)
		source_app_id = request["header_map"][self.SOURCE_APP_ID_KEY]
		locale = query_params.get("locale", "en-US")
		job_id = job_id = str(uuid.uuid4())

		should_log_telemetry, _ = self._fetch_telemetry_details(service, request, "personalizedmetadata")

		saia_api = SaiaApi(service, system_scoped_service, user, "personalizedmetadata", hashed_user)

		search_req =  self.handle_search_request(
			saia_api,
			query_params,
			job_id,
			locale,
			should_log_telemetry,
			source_app_id,
		)

		return search_req
