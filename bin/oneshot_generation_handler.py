# Copyright 2024 Splunk Inc.
import json
import sys
import os
import time
import uuid

from splunk.persistconn.application import PersistentServerConnectionApplication

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from base_rest import BaseRestUtils

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from spl_gen.utils import log_kwargs, get_app_version, deterministic_hash
from spl_gen.utils.audit_logging import generate_chat_audit_log, generate_ai_service_usage_log
from spl_gen.remote.v1alpha1 import SaiaApi

class OneshotGenerationHandler(PersistentServerConnectionApplication, BaseRestUtils):

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

    # SPL parsing functions
    def filterExtraSPLDelim(self, result, delimiter_sequences):
        """Helper function to filter out extra SPL delimeters"""
        filteredRes = result
        for open_delim, close_delim in delimiter_sequences:
            filteredRes = filteredRes.lstrip(close_delim)
        return filteredRes

    def get_spl(self, prompt, open_delim, close_delim, delimiter_sequences, result = []):
        """Recurssive func that gets the spl from the result prompt given open and close delimeters"""
        res = prompt.split(open_delim, 1)
        if len(res) == 2:
            res = res[1].split(close_delim, 1)
            if len(res) == 2:
                if (res[0] != ''):
                    result.append(res[0].strip(' \n'))
                self.get_spl(res[1], open_delim, close_delim, delimiter_sequences, result)
                if res[0] == '':
                    filteredRes = self.filterExtraSPLDelim(res[1], delimiter_sequences)
                    result.append(filteredRes)
                return result
        return result

    def parse_spl(self, res_text, delimiter_sequences):
        """ Parse spl from result text given a set of delimeters """
        for sequence in delimiter_sequences:
            open_delimiter = sequence[0]
            close_delimiter = sequence[1]
            res = self.get_spl(res_text, open_delimiter, close_delimiter, delimiter_sequences, [])
            if len(res) != 0 and not (len(res) == 1 and res[0] == ''):
                return res
        # No match case
        return []

    # For unit testing, put actual generation logic into separate function
    def handle_oneshot_generation(self, saia_api, params, job_id, locale, should_log_telemetry, source_app_id, system_scoped_service, user):
        if "prompt" not in params:
            return self.create_response({"error": "Incorrect arguments provided"}, 400)

        user_prompt = params["prompt"]
        spl_only = False
        if "spl_only" in params:
            spl_only = params["spl_only"]

        # Make request directly to SAIA SCS API, for oneshot generation
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
			rag_data_only=False,
			rewrite_content=False,
			indexed_fields=[],
			use_state_streamer=False,
        )

        final_result = ''
        if spl_only:
            # Parse the SPL block out of the response text
            # Special case: sometimes model generates ```spl<some spl>```
            delimiter_sequences = [
                ('```spl', '```'),
                ('`spl', '`'),
                ('```', '```'),
                ('`', '`'),
            ]
            parsed_spl = self.parse_spl(result.text, delimiter_sequences)
            if len(parsed_spl) > 0:
                final_result = parsed_spl
            else:
                # If there is no SPL block, return the entirety of the result text
                final_result = result.text
        else:
            # Return text directly
            final_result = result.text

        end_time = time.time()

        generate_chat_audit_log(
            system_scoped_service,
            request_id=job_id,
            chat_id='oneshot',
            user=user,
            role="assistant",
            content=final_result,
        )

        generate_ai_service_usage_log(
            system_scoped_service,
            request_id=job_id,
            time=end_time,
            user=user,
            prompt_types=['Write SPL'],
            status=200,
            source_app_id=source_app_id
        )

        return self.create_response(json.dumps({"response": final_result}), 200)


    def handle_func(self, request):
        # Handle a syncronous from splunkd.
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

        should_log_telemetry, _ = self._fetch_telemetry_details(service, request, "oneshot")

        self.logger.info(
            log_kwargs(
                UUID=job_id,
                user=hashed_user,
                source_app=ns["app"], # TODO: Is this sufficient for provenance tracking?
                chat_id="oneshot",
                message="Generating one-shot SPL",
                saia_app_version=app_version,
            )
        )

        saia_api = SaiaApi(service, system_scoped_service, user, "oneshot", hashed_user)
        return self.handle_oneshot_generation(
            saia_api,
            params,
            job_id,
            locale,
            should_log_telemetry,
            source_app_id,
            system_scoped_service,
            user,
        )
