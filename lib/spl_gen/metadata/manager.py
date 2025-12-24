from datetime import datetime
from requests import HTTPError

from .collection import MetadataCollection
from splunklib.searchcommands import environment
from ..constants import DATA_CONFIG_FEATURE_MAP
from ..utils import log_kwargs, get_updated_feature_settings, update_saved_searches_from_feature_settings, update_modular_inputs_from_feature_settings, get_app_version
class MetadataManager:
    def __init__(self, service, metadata_collection, saia_api, username=None, locale=None):
        try:
            self.logger, _ = environment.configure_logging(self.__class__.__name__)
            self.service = service
            self.metadata_collection = metadata_collection
            self.username = username
            self.locale = locale
            self.api = saia_api
            self.hashed_user = hash(username)
            self.app_version = get_app_version(service)
        except KeyError:
            raise Exception("KVStore collection not found")

    def _should_fetch_metadata(self, metadata: dict):
        last_updated = metadata[MetadataCollection.KEY_LAST_UPDATED]
        if not last_updated:
            return True

        time_delta = datetime.utcnow() - datetime.fromisoformat(last_updated)
        if time_delta.seconds < 60 * 15:
            # Has been less than 15 minutes since last update
            return False

        return True

    def _get_updated_metadata(self, request_id):
        try:
            response = self.api.metadata(request_id)
            response_body = response.json()

            service_warning = ""
            for key, value in response.headers.items():
                if key.lower().startswith("warning-"):
                    # I assume there's just one warning coming through
                    # and we want to surface that
                    service_warning = value
                    break

            return {
                "versions": response_body["versions"],
                "remote_enabled_features": response_body.get("enabled_features", []),
                "orchestrator_enabled": response_body.get("orchestrator_enabled", False),
                "service_warning": service_warning
            }
        except Exception as e:
            if isinstance(e, HTTPError):
                self.logger.error(
                    log_kwargs(
                        message=f"Received an error with status code {e.response.status_code}",
                        user=self.hashed_user,
                        saia_app_version=self.app_version,
                        request_id=request_id,
                    )
                )
            else:
                self.logger.error(
                    log_kwargs(
                        message=f"unknown exception {repr(e)}",
                        user=self.hashed_user,
                        saia_app_version=self.app_version,
                        request_id=request_id,
                    )
                )

    def _update_data_statuses(self, feature_settings, request_id):
        to_refresh = []
        for key, entry in feature_settings.items():
            # Guard against feature flags that don't exist in this version
            if key not in DATA_CONFIG_FEATURE_MAP:
                self.logger.warning(
                    log_kwargs(
                        message=f"Skipping unknown feature flag: {key}",
                        request_id=request_id,
                    )
                )
                continue

            data_ids = DATA_CONFIG_FEATURE_MAP[key]
            for data_id in data_ids:
                should_refresh = self.api.update_data_status(data_id, not entry.get("enabled", False), request_id)
                if should_refresh:
                    to_refresh.append(data_id)
        return to_refresh

    def _create_new_metadata_entry(self, existing_entry, response_content):
        last_updated_entry = datetime.utcnow().isoformat()
        version_config_entry = {"versions": response_content["versions"]}
        updated_feature_settings = get_updated_feature_settings(existing_feature_settings=existing_entry[MetadataCollection.KEY_FEATURE_SETTINGS], remote_enabled_features=response_content["remote_enabled_features"])

        return {MetadataCollection.KEY_VERSION_INFO: version_config_entry, MetadataCollection.KEY_FEATURE_SETTINGS: updated_feature_settings, MetadataCollection.KEY_LAST_UPDATED: last_updated_entry, MetadataCollection.KEY_ORCHESTRATOR_ENABLED: response_content["orchestrator_enabled"]}

    def _update_knowledge_objects(self, feature_settings, request_id, data_to_refresh = None):
        try:
            update_saved_searches_from_feature_settings(feature_settings, self.service, data_to_refresh)
            update_modular_inputs_from_feature_settings(feature_settings, self.service)
        except Exception as e:
            error_msg = f"An exception occurred while attempting to update saved searches based on latest feature settings: {repr(e)}"
            self.logger.error(
                log_kwargs(
                    request_id=request_id,
                    message=error_msg,
                )
            )

    def get(self, request_id, fetch_latest=False):
        result = self.metadata_collection.get()
        if fetch_latest:
            if not all([self.username, self.locale]):
                raise ValueError("username and locale must be set to fetch latest metadata")

            metadata_response_content = self._get_updated_metadata(request_id)
            if metadata_response_content:
                service_warning = metadata_response_content["service_warning"]

                updated_entry = self._create_new_metadata_entry(result, metadata_response_content)
                self.metadata_collection.update(updated_entry)

                self._update_knowledge_objects(updated_entry[MetadataCollection.KEY_FEATURE_SETTINGS], request_id)

                result = {**updated_entry, "service_warning": service_warning}

        return result

    def update(self, request_id, entry):
        updated_feature_settings = entry.get(MetadataCollection.KEY_FEATURE_SETTINGS)
        if updated_feature_settings:
            # Need to determine what data should be updated right away and update saved searches
            data_to_refresh = self._update_data_statuses(updated_feature_settings, request_id)
            self._update_knowledge_objects(updated_feature_settings, request_id, data_to_refresh)

        result = self.metadata_collection.update(entry)

        return result
