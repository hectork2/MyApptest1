import json
import logging

class SaiaFieldSummaryCollection:
    def __init__(self, service):
        try:
            self.logger = logging.Logger(self.__class__.__name__)
            self.saia_collection_data = service.kvstore["saia_field_summary"]
        except KeyError:
            raise Exception("KVStore collection not found")

    def get(self):
        results = self.saia_collection_data.data.query()
        return results

    def query(self, index, sourcetype):
        results = self.saia_collection_data.data.query(
            query={"index": index, "sourcetype": sourcetype}
        )
        if len(results) == 0:
            return None
        else:
            ret_obj = results[0]
            if isinstance(ret_obj["fields_obj"], str):
                ret_obj["fields_obj"] = json.loads(ret_obj["fields_obj"])
            
            return ret_obj

    def insert(self, index, sourcetype, fields_obj):
        return self.saia_collection_data.data.insert(
            {"index": index, "sourcetype": sourcetype, "fields_obj": fields_obj}
        )

    def update(self, key, index, sourcetype, fields_obj):
        return self.saia_collection_data.data.update(
            key, {"index": index, "sourcetype": sourcetype, "fields_obj": fields_obj}
        )

    def clear(self, index, sourcetype):
        return self.saia_collection_data.data.delete(
            query=json.dumps({"index": index, "sourcetype": sourcetype})
        )

    def batch_save(self, entries):
        return self.saia_collection_data.data.batch_save(
            *entries
        )