import json
import logging
import time

class SaiaJobMapCollection:
    def __init__(self, service):
        try:
            self.logger = logging.Logger(self.__class__.__name__)
            self.saia_collection_data = service.kvstore["saia_job_map"]
        except KeyError:
            raise Exception("KVStore collection not found")

    def get_all_jobs(self):
        results = self.saia_collection_data.data.query()
        return results

    def get_job(self, job_id):
        try:
            result = self.saia_collection_data.data.query_by_id(job_id)
            return result
        except Exception as e:
            logging.info(f"Caught error {e}")
            return None

    def get_pending_jobs(self):
        results = self.saia_collection_data.data.query(
            query={"job_status": "pending"}
        )
        return results

    def get_running_jobs(self):
        results = self.saia_collection_data.data.query(
            query={"job_status": "running"}
        )
        return results

    def get_completed_jobs(self):
        results = self.saia_collection_data.data.query(
            query={"job_status": "completed"}
        )
        return results

    def insert_job(self, job_id, job_payload):
        return self.saia_collection_data.data.insert(
            {"_key": job_id, "job_status": "pending", "job_payload": job_payload, "last_updated": time.time()}
        )

    def update_job(self, job_id, job_status):
        return self.saia_collection_data.data.update(
            job_id, {"job_status": job_status, "last_updated": time.time()}
        )

    def delete_job(self, job_id):
        return self.saia_collection_data.data.delete_by_id(job_id)

    def cleanup_completed_jobs(self):
        results = self.saia_collection_data.data.delete(
            query=json.dumps({"job_status": "completed"})
        )
        return results
    
    def delete_running_jobs(self):
        results = self.saia_collection_data.data.delete(
            query=json.dumps({"job_status": "running"})
        )
        return results
