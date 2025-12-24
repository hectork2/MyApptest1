import uuid
import sys, os, json
import time
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from splunklib.searchcommands import dispatch, EventingCommand, Configuration
from spl_gen.remote.v1alpha1 import SaiaApi
from spl_gen.utils import log_kwargs, get_app_version


def adjust_record(record):
    del record["values"]  # Drop this data due to potentially large size
    record["is_numeric"] = record["numeric_count"] == "0"


@Configuration()
class SubmitFieldDataCommand(EventingCommand):
    def transform(self, records):
        # system_scoped_service is same as self.service since this command should only get
        # run by the splunk_system_role as a part of the field summary saved search
        api = SaiaApi(service=self.service, system_scoped_service=self.service, username=self.metadata.searchinfo.username) # pyright: ignore
        app_version = get_app_version(self.service)
        self.logger.info( # pyright: ignore
            log_kwargs(
                message="Submitting index metadata.",
                saia_app_version=app_version,
            )
        ) 

        field_data = []
        stack_id = None

        record_id_template = "{index},{sourcetype}"
        unique_records = set()
        for record in records:
            adjust_record(record)
            unique_records.add(record_id_template.format(index=record["index"], sourcetype=record["sourcetype"]))
            field_data.append(json.dumps(record))

        request_id = str(uuid.uuid4())
        self.logger.info( # pyright: ignore
            log_kwargs(
                request_id=request_id,
                anticipated_entry_count=len(unique_records),
                saia_app_version=app_version,
            )
        )

        response_text = None
        # We only want to run the saved search and send data for Splunk Cloud deployments
        # TODO: If we ever go on-prem with SAIA, need to detect & disable for on-prem stacks
        try:
            response = api.submit_sourcetype_metadata(field_data=field_data, request_id=request_id)
            response.raise_for_status()
            response_text = response.text
            self.logger.info(  # pyright: ignore
                log_kwargs(
                    message="Index metadata submitted successfully.",
                    saia_app_version=app_version,
                ))
        except requests.RequestException as ex:
            response_text = f"Failed to POST index metadata: {ex.response.text}" # pyright: ignore
            self.logger.error(  # pyright: ignore
                log_kwargs(
                    message=response_text,
                    saia_app_version=app_version,
                )
            )

        yield {"_time": time.time(), "response": response_text}


dispatch(SubmitFieldDataCommand, sys.argv, sys.stdin, sys.stdout, __name__)
