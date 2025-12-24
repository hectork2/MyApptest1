[status]
last_context_update_timestamp = <integer>
* Unix timestamp of the last successful SPL context update.
* Represents when the update process started. Defaults to 0.

[settings]
context_update_interval_seconds = <integer>
* Minimum time interval, in seconds, for periodic SPL context updates.
* Defaults to 0.

max_search_range_seconds = <integer>
* Defines the maximum time span, in seconds, for search queries.
* Default: 604800 (7 days).

max_chunk_range_seconds = <integer>
* Specifies the maximum time range, in seconds, for a single SPL update request.
* Default: 86400.

[search_params]
min_minimum_event_count_per_tuple = <integer>
* lower bound on the minimum event count per index, sourcetype, size tuple , for field summaries context query.
* Defaults to 10.

max_minimum_event_count_per_tuple = <integer>
* upper bound on the minimum event count per index, sourcetype, size tuple , for field summaries context query.
* Defaults to 50.

maximum_event_batch_size = <integer>
* Defines the maximum number of events looked at for the each stats search, in seconds, for field summaries context query.
* Default: 15000

target_search_count = <integer>
* Specifies the target number of searches run for field summaries context.
* Default: 20.

maximum_search_count = <integer>
* Specifies the maximum number of searches run for field summaries context.
* Default: 100.

maximum_tuples_for_search = <integer>
* Specifies the maximum number of index, sourcetype pairs used for each field summaries context query.
* Default: 500.

maximum_count_no_sampling = <integer>
* Specifies the maximum number of events in queried with no sampling, for field summaries context query.
* Default: 100.

maximum_events_search_count = <integer>
* Specifies the maximum number of events in queried with no sampling, for field summaries context query.
* Default: 7000000000.
