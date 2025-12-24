import logging
from abc import ABC, abstractmethod

class SingleEntryCollection(ABC):
    def __init__(self, service):
        try:
            self.service = service
            self.collection_data = service.kvstore[self.collection_name]
        except KeyError:
            raise Exception("KVStore collection not found")
        self.logger = logging.getLogger(self.__class__.__name__)
        
    @property
    @abstractmethod
    def collection_name(self) -> str:
        pass
        
    @property
    @abstractmethod
    def default_entry(self) -> dict:
        pass

    @property
    @abstractmethod
    def keys(self) -> list:
        pass

    def _parse_res(self, response) -> dict:
        ret = {}
        for key in self.keys:
            if key in response:
                ret[key] = response[key]
            else:
                ret[key] = self.default_entry.get(key, False)
        return ret

    def _insert(self, entry):
        return self.collection_data.data.insert(
            entry
        )

    def check_keys_valid(self, entry):
        invalid_keys = [key for key in entry.keys() if key not in self.keys]
        if invalid_keys:
            raise Exception(f"Invalid keys {invalid_keys} for collection {self.collection_name}")
        
    def _get(self):
        results = self.collection_data.data.query()
        if len(results) == 0:
            key_dict = self._insert(self.default_entry)

            result = self.collection_data.data.query_by_id(
                key_dict["_key"]
            )
        else:
            result = results[0]
        
        return self._parse_res(result)

    def _update(self, entry):
        self.check_keys_valid(entry)
        existing_entry = self.get()
        self.clear()
        to_insert = {**existing_entry, **entry}
        self._insert(
            to_insert
        )
        return to_insert
    
    def _run_with_retries(self, func, kwargs=None, max_retries=1):
        for i in range(max_retries + 1):
            try:
                result = func(**kwargs) if kwargs else func()
                return result
            except Exception as e:
                self.logger.error(f"Retrying after an exception occurred for a KV Store operation: {repr(e)}")
                if i == max_retries:
                    raise e

    def get(self):
        return self._run_with_retries(self._get)
    
    def update(self, entry):
        return self._run_with_retries(self._update, dict(entry=entry))
    
    def clear(self):
        return self.collection_data.data.delete()