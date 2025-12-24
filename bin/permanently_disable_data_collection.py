import sys, os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from splunklib.searchcommands import dispatch, EventingCommand, Configuration
from spl_gen.user_settings.collection import SettingsCollection
from spl_gen.utils import log_kwargs, get_app_version

@Configuration()
class PermanentlyDisableAIServiceDataCollectionCommand(EventingCommand):
    def transform(self, records):
        app_version = get_app_version(self.service)
        self.logger.info( # pyright: ignore
            log_kwargs(
                message="Permanently disabling AI Service Data.",
                saia_app_version=app_version,
            )
        ) 
        # system_scoped_service is same as self.service since this command should only get
        # run by the splunk_system_role as a part of the field summary saved search
        saia_settings = SettingsCollection(self.service)
        
        saia_settings.update({
            SettingsCollection.KEY_PERMANENTLY_DISABLE_AI_SERVICE_DATA: True 
        })

        yield {"_time": time.time(), "response": "OK"}


dispatch(PermanentlyDisableAIServiceDataCollectionCommand, sys.argv, sys.stdin, sys.stdout, __name__)
