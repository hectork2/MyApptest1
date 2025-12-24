import json
import logging

class SaiaIndexedFieldsCollection:
    def __init__(self, service):
        try:
            self.logger = logging.Logger(self.__class__.__name__)
            self.saia_indexed_fields_collection_data = service.kvstore["saia_indexed_fields"]
        except KeyError:
            raise Exception("KVStore collection not found")

    def get(self):
        results = self.saia_indexed_fields_collection_data.data.query()
        for result in results:
            if isinstance(result["indexed_fields"], str):
                result["indexed_fields"] = json.loads(result["indexed_fields"])
        return results

    def query(self, index):
        try:
            ret_obj = self.saia_indexed_fields_collection_data.data.query_by_id(index)
            if isinstance(ret_obj["indexed_fields"], str):
                ret_obj["indexed_fields"] = json.loads(ret_obj["indexed_fields"])
            return ret_obj
        except Exception as e:
            logging.info(f"Caught error {e}")
            return None

    def insert(self, index, indexed_fields):
        return self.saia_indexed_fields_collection_data.data.insert(
            {"_key": index, "indexed_fields": indexed_fields}
        )

    def update(self, index, indexed_fields):
        return self.saia_indexed_fields_collection_data.data.update(
            index, {"indexed_fields": indexed_fields}
        )

    def clear(self, index):
        return self.saia_indexed_fields_collection_data.data.delete_by_id(
            index
        )

    def batch_save(self, entries):
        return self.saia_indexed_fields_collection_data.data.batch_save(
            *entries
        )