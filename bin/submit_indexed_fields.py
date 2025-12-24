import uuid
import sys, os, json
import time
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from splunklib.searchcommands import dispatch, EventingCommand, Configuration
from spl_gen.remote.v1alpha1 import SaiaApi
from spl_gen.saia_indexed_fields_collection import SaiaIndexedFieldsCollection
from spl_gen.utils import reset_saved_search_time, log_kwargs, get_app_version
from spl_gen.constants import SAVED_SEARCH_INDEXED_FIELDS

@Configuration()
class SubmitIndexedFieldDataCommand(EventingCommand):
    def transform(self, records):
        # system_scoped_service is same as self.service since this command should only get
        # run by the splunk_system_role as a part of the field summary saved search
        indexed_fields_collection = SaiaIndexedFieldsCollection(self.service)
        app_version = get_app_version(self.service)
        self.logger.info( # pyright: ignore
            log_kwargs(
                message="Submitting index metadata.",
                saia_app_version=app_version,
            )
        ) 

        indexed_fields_map = {}
        for record in records:
            if record["index"] not in indexed_fields_map:
                indexed_fields_map[record["index"]] = []
            indexed_fields_map[record["index"]].append(record["field"])

        request_id = str(uuid.uuid4())
        self.logger.info( # pyright: ignore
            log_kwargs(
                message="gathered indexed fields list",
                request_id=request_id,
                anticipated_entry_count=len(indexed_fields_map.keys()),
                saia_app_version=app_version,
            )
        )

        response_text = None
        # For Optimization preview, this saved search writes indexed fields to a KVStore collection
        try:
            existing_indexed_fields = indexed_fields_collection.get()
            existing_indexes = {}
            for indexed_field_list in existing_indexed_fields:
                existing_indexes[indexed_field_list["_key"]] = 1
                if indexed_field_list["_key"] in indexed_fields_map:
                    new_list = list(set(indexed_field_list["indexed_fields"] + indexed_fields_map[indexed_field_list["_key"]]))
                    indexed_fields_map[indexed_field_list["_key"]] = new_list
                else:
                    indexed_fields_map[indexed_field_list["_key"]] = indexed_field_list["indexed_fields"]

            for key, values in indexed_fields_map.items():
                if key in existing_indexes:
                    indexed_fields_collection.update(key, list(set(values)))
                else:
                    indexed_fields_collection.insert(key, list(set(values)))

            self.logger.info(  # pyright: ignore
                log_kwargs(
                    message="Indexed fields list submitted successfully.",
                    saia_app_version=app_version,
                ))
            reset_saved_search_time(self.service, SAVED_SEARCH_INDEXED_FIELDS)
        except Exception as ex:
            response_text = f"Failed to saved indexed fields list: {ex}" # pyright: ignore
            self.logger.error(  # pyright: ignore
                log_kwargs(
                    message=response_text,
                    saia_app_version=app_version,
                )
            )

        yield {"_time": time.time(), "response": response_text}


dispatch(SubmitIndexedFieldDataCommand, sys.argv, sys.stdin, sys.stdout, __name__)
