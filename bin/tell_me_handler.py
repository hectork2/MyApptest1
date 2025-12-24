import json
import os
import sys
import time
import uuid

from splunk.persistconn.application import PersistentServerConnectionApplication

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from base_rest import BaseRestUtils

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from spl_gen.utils import log_kwargs, get_app_version, deterministic_hash
from spl_gen.utils.audit_logging import generate_chat_audit_log, generate_ai_service_usage_log
from spl_gen.remote.v1alpha1 import SaiaApi


class OneshotTellMeHandler(PersistentServerConnectionApplication, BaseRestUtils):
    def __init__(self, _command_line, _command_arg):
        super(PersistentServerConnectionApplication, self).__init__()
        super(BaseRestUtils, self).__init__()

    def handle(self, in_bytes):
        return self.handle_wrapper(in_bytes, self.handle_func, require_source_app_id=False)

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

    def handle_oneshot_tellme_generation(self, saia_api, params, job_id, locale, should_log_telemetry, source_app_id, system_scoped_service, user):
        """
        Handle the oneshot-tellme generation request.
        """
        if "prompt" not in params:
            return self.create_response({"error": "Incorrect arguments provided"}, 400)

        user_prompt = params["prompt"]

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
			classification=2,
			locale=locale,
			log_to_telemetry=should_log_telemetry,
			was_chat_empty=False,
			source_app_id=source_app_id,
			ast="",
			rag_data_only=False,
			rewrite_content=False,
			indexed_fields=[],
			use_state_streamer=False,
        )

        end_time = time.time()

        # Collect resource links from response headers if provided by service
        resource_links = []
        try:
            source_urls = None
            source_titles = None
            for key, value in result.headers.items():
                k = key.lower()
                if k == "metadata-source-urls":
                    source_urls = json.loads(value)
                if k == "metadata-source-titles":
                    source_titles = json.loads(value)

            if source_urls and source_titles and len(source_urls) == len(source_titles):
                resource_links = [
                    {"href": u, "description": t} for u, t in zip(source_urls, source_titles)
                ]
        except Exception:
            # Best effort; do not block on link extraction
            pass

        generate_chat_audit_log(
            system_scoped_service,
            request_id=job_id,
            chat_id='oneshot',
            user=user,
            role="assistant",
            content=result.text,
        )

        generate_ai_service_usage_log(
            system_scoped_service,
            request_id=job_id,
            time=end_time,
            user=user,
            prompt_types=['Tell me about'],
            status=200,
            source_app_id=source_app_id
        )

        return self.create_response(json.dumps({"response": result.text, "resourceLinks": resource_links}), 200)


    def handle_func(self, request):
        service = self.service_from_request(request)
        system_scoped_service = self.service_from_request(request, use_system_token=True)
        app_version = get_app_version(system_scoped_service)
        session = request["session"]
        user = session["user"]
        hashed_user = deterministic_hash(user)
        query_params = self.get_query_params(request)
        job_id = str(uuid.uuid4())
        source_app_id = request["header_map"][self.SOURCE_APP_ID_KEY]
        request_payload = json.loads(request.get("payload", "{}"))
        params = request_payload if request_payload else query_params

        ns = request["ns"]
        if "lang" in request:
            locale = request["lang"]
        else:
            locale = "en-US"

        should_log_telemetry, _ = self._fetch_telemetry_details(service, request, "oneshot-tell-me")

        self.logger.info(
            log_kwargs(
                UUID=job_id,
                user=hashed_user,
                source_app=ns["app"],
                chat_id="oneshot-tell-me",
                message="Generating one-shot-tell-me SPL",
                saia_app_version=app_version,
            )
        )

        saia_api = SaiaApi(service, system_scoped_service, user, "oneshot-tell-me", hashed_user)
        return self.handle_oneshot_tellme_generation(
            saia_api,
            params,
            job_id,
            locale,
            should_log_telemetry,
            source_app_id,
            system_scoped_service,
            user,
        )
