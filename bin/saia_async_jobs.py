# Copyright 2025 Cisco, Inc.
import argparse
import json
import logging
import os
import random
import re
import sys
import splunk
import time
import uuid


from collections import namedtuple
from datetime import datetime, timezone
from typing import Any, List, Dict, Optional, Sequence, cast

from splunk import SplunkdConnectionException, ResourceNotFound

from base_rest import BaseRestUtils

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from spl_gen.remote.v1alpha1 import SaiaApi
from spl_gen.saia_job_map_collection import SaiaJobMapCollection
from spl_gen.utils import log_kwargs, get_app_version
from spl_gen.utils.modinput.json_modinput import JsonModularInput
from spl_gen.utils.modinput.fields import BooleanField
from spl_gen.utils.requests import send_request, OutputMode
from spl_gen.utils.log import setup_logger, add_log_extra_metadata
from spl_gen.remote.jobs import AsyncHttpJobs

from splunklib.searchcommands import environment
from splunklib.client import connect
from splunklib.binding import handler


logger = setup_logger("saia_async_jobs_modinput")
LOGGER_METADATA_TAG = "saia_async_jobs_modinput"

class SAIAAsyncJobsModularInput(JsonModularInput, BaseRestUtils):
    """
    A class responsible for managing async jobs (namely streaming HTTP requests to SAIA service)
    """
    def __init__(self, this_logger: logging.Logger = logger):
        environment.app_root = os.path.join(os.path.dirname(__file__), "..")
        scheme_args = {
            "title": "Douran Assitant for SPL Async Jobs Modular Input",
            "description": "Runs streaming HTTP requests to SAIA service",
            "use_external_validation": "true",
            "streaming_mode": "json",
            "use_single_instance": "true",
            "supported_on_cloud": "true",
            "supported_on_onprem": "true",
            "supported_on_fedramp": "true",
            "requires_kvstore": "true",
            "kvstore_wait_time": 30,
            "run_only_on_captain": "false",
            # "skip_wait_for_es": "true", # Doesn't matter, we're not ES
        }
        self._chunk_count = 0
        self.search_params = dict()
        args = [BooleanField("debug", "Debug", "If true, debug logging is enabled.", required_on_create=False)]
        super().__init__(scheme_args, args, this_logger)



    def run(self, stanza: Sequence[Dict[str, Any]]):
        self.logger.info("Starting Async Jobs Modular Input")
        self.logger.setLevel(self.get_log_level(stanza))

        jobs = AsyncHttpJobs()

        # Create service for SaiaApi object
        user = "splunk-system-user"
        self.logger.info("Connecting to SAIA service")
        system_scoped_service = connect(
            token=self._input_config.session_key,
            handler=handler(timeout=1),
            host="127.0.0.1",
            app="Splunk_AI_Assistant_Cloud",
            owner=user,
            retries=2
        )

        job_map_collection = SaiaJobMapCollection(system_scoped_service)

        # Cleanup any completed jobs in the job map collection
        logger.info("Cleaning up completed jobs in the job map collection")
        job_map_collection.cleanup_completed_jobs()

        # For now, delete any still-running jobs in the job map collection (they came from a previous modinput process so they are dead now)
        # This is a temporary measure until we implement job retry on Splunk app side
        logger.info("Deleting any running jobs in the job map collection")
        job_map_collection.delete_running_jobs()

        # TODO: On startup, check the job map collection for any running jobs, and if any are found, spawn threads for them
        # resuming those jobs as the modinput process was killed
        # running_jobs = job_map_collection.get_running_jobs()
        # if running_jobs:
        #     self.logger.info(f"Found {len(running_jobs)} running jobs, resuming them")
        #     for job in running_jobs:
        #         job_id = job["_key"]
        #         job_payload = job["job_payload"]
                # self.logger.info(f"Resuming job {job_id} with payload {job_payload}")

        while True:
            # Poll 120 times a minute
            time.sleep(0.5)
            # Read pending jobs from the job map collection, spawn threads for each pending job
            # Job streams on a spawned thread and writes the results to the KVStore saia_collection_v2 in corresponding chat
            pending_jobs = job_map_collection.get_pending_jobs()
            if pending_jobs:
                logger.info(f"Found {len(pending_jobs)} pending jobs, processing them")
                for job in pending_jobs:
                    job_id = job["_key"]
                    job_payload = job["job_payload"]

                    logger.info(f"Processing job {job_id}")
                    job_map_collection.update_job(job_id, "running")
                    logger.info(f"Updated job {job_id} to running")

                    job_request = {
                        "session": {
                            "authtoken": job_payload["token"],
                            "user": job_payload["user"]
                        },
                        "ns": {
                            "app": job_payload["app_name"]
                        }
                    }

                    service = self.service_from_request(job_request)

                    if "tool_id" in job_payload:
                        tool_id = job_payload["tool_id"]
                    else:
                        tool_id = None

                    job_id = jobs.create_from_request(
                        app_name=job_payload["app_name"],
                        user=job_payload["user"],
                        hashed_user=job_payload["hashed_user"],
                        kwargs={
                            "user_prompt": job_payload["user_prompt"],
                            "chat_history": job_payload["chat_history"],
                            "classification": job_payload["classification"],
                            "locale": job_payload["locale"],
                            "log_to_telemetry": job_payload["log_to_telemetry"],
                            "was_chat_empty": job_payload["was_chat_empty"],
                            "source_app_id":  job_payload["source_app_id"],
                            "rewrite_content":  job_payload["rewrite_content"],
                        },
                        service=service,
                        system_scoped_service=system_scoped_service,
                        chat_id=job_payload["chat_id"],
                        response_id=job_payload["response_id"],
                        app_version=job_payload["app_version"],
                        should_log_telemetry=job_payload["log_to_telemetry"],
                        source_app_id=job_payload["source_app_id"],
                        tool_id=tool_id,
                    )
                    self.logger.info(f"Processing job {job_id}")



if __name__ == '__main__':
    add_log_extra_metadata("tag", LOGGER_METADATA_TAG)
    add_log_extra_metadata("context", "modinput")
    mod_input = SAIAAsyncJobsModularInput()
    mod_input.execute()
