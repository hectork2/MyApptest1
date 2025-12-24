import logging

from ..utils import get_enabled_feature_flags
from ..collection import SingleEntryCollection

class MetadataCollection(SingleEntryCollection):
    KEY_VERSION_INFO = "version_info"
    KEY_FEATURE_SETTINGS = "feature_settings"
    KEY_LAST_UPDATED = "last_updated"
    KEY_ORCHESTRATOR_ENABLED = "orchestrator_enabled"

    def __init__(self, service):
        super().__init__(service)
        
    @property
    def collection_name(self):
        return "saia_metadata"
    
    @property
    def default_entry(self):
        return {
            self.KEY_FEATURE_SETTINGS: {},
            self.KEY_VERSION_INFO: {},
            self.KEY_LAST_UPDATED: ""
        }
    
    @property
    def keys(self):
        return [self.KEY_VERSION_INFO, self.KEY_FEATURE_SETTINGS, self.KEY_LAST_UPDATED, self.KEY_ORCHESTRATOR_ENABLED]
    
    def get_enabled_feature_flags(self):
        enabled_flags = []
        feature_settings_entry = {}
        metadata = self.get()
        if self.KEY_FEATURE_SETTINGS in metadata:
            feature_settings_entry = metadata[self.KEY_FEATURE_SETTINGS]
            
            enabled_flags = get_enabled_feature_flags(feature_settings_entry)
        
        return enabled_flags, feature_settings_entry
