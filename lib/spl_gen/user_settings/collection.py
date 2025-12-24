import logging

from ..collection import SingleEntryCollection
from ..utils import update_saved_searches_from_ai_service_data_enabled_setting

class SettingsCollection(SingleEntryCollection):
    KEY_AI_SERVICE_DATA_ENABLED = "ai_service_data_enabled"
    KEY_PERMANENTLY_DISABLE_AI_SERVICE_DATA = "permanently_disable_ai_service_data"

    def __init__(self, service):
        super().__init__(service)

    @property
    def collection_name(self):
        return "saia_settings"

    @property
    def default_entry(self):
        return {
            self.KEY_AI_SERVICE_DATA_ENABLED: False,
            self.KEY_PERMANENTLY_DISABLE_AI_SERVICE_DATA: False,
        }

    @property
    def keys(self):
        return [self.KEY_AI_SERVICE_DATA_ENABLED, self.KEY_PERMANENTLY_DISABLE_AI_SERVICE_DATA]

    def update(self, entry):
        self.check_keys_valid(entry)

        # Update saved searches when ai_service_data_enabled are turned on/off
        if self.KEY_AI_SERVICE_DATA_ENABLED in entry:
            update_saved_searches_from_ai_service_data_enabled_setting(
                entry[self.KEY_AI_SERVICE_DATA_ENABLED],
                self.service
            )

        return super().update(entry)
