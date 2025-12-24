#### Splunk AI Assistant Environment Context ####
[saia_field_summary://default]
* Responsible for collecting field summary personalization context for SAIA SPL generation

debug = <boolean>
* If true, debug logging is enabled.
* Default: false

#### Splunk AI Assistant Async Job Map Modinput ####
[saia_async_jobs://default]
* Responsible for reading jobs from saia_job_map collection, running HTTP streaming connections to SCS, writing results to KVStore

debug = <boolean>
* If true, debug logging is enabled.
* Default: false