# Copyright 2025 Cisco, Inc.
import argparse
import json
import logging
import os
import random
import re
import sys
import splunk
import time
import uuid


from collections import namedtuple
from datetime import datetime, timezone
from typing import Any, List, Dict, Optional, Sequence, cast

from splunk import SplunkdConnectionException, ResourceNotFound

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from spl_gen.remote.v1alpha1 import SaiaApi
from spl_gen.saia_field_summary_collection import SaiaFieldSummaryCollection
from spl_gen.utils import log_kwargs, get_app_version, deterministic_hash
from spl_gen.utils.splunk_search import create_search_job, get_search_results
from spl_gen.utils.modinput.json_modinput import JsonModularInput
from spl_gen.utils.modinput.fields import BooleanField
from spl_gen.utils.requests import send_request, OutputMode
from spl_gen.utils.log import setup_logger, add_log_extra_metadata

from splunklib.searchcommands import environment
from splunklib.client import connect
from splunklib.binding import handler

FIELD_SUMMARIES_CONTEXT_NAME = "spl_field_summaries"

SECONDS_IN_DAY = 86400
DEFAULT_MAX_SEARCH_RANGE_SECONDS = 7 * SECONDS_IN_DAY
DEFAULT_MAX_CHUNK_RANGE_SECONDS = SECONDS_IN_DAY

# default Values for search parameters
D_MIN_MINIMUM_EVENT_COUNT_PER_TUPLE = 10
D_MAX_MINIMUM_EVENT_COUNT_PER_TUPLE = 50
D_MAXIMUM_EVENT_BATCH_SIZE = 15000
D_TARGET_SEARCH_COUNT = 20
D_MAXIMUM_SEARCH_COUNT = 100
D_MAXIMUM_TUPLES_FOR_SEARCH = 500
D_MAXIMUM_COUNT_NO_SAMPLING = 100
D_MAXIMUM_EVENTS_SEARCH_COUNT = 7000000000
LARGEST_SAMPLING_RATIO = 100000

SA_CONF_URL = f"{splunk.rest.makeSplunkdUri()}servicesNS/nobody/Splunk_AI_Assistant_Cloud/configs/conf-saiafieldsummary"

logger = setup_logger("saia_field_summary_modinput")
LOGGER_METADATA_TAG = "saia_field_summary_modinput"

def time_modifier_from_timestamp(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


Macro = namedtuple("Macro", ["name", "use_time_range"])
TimeModifiers = namedtuple("TimeModifiers", ["earliest", "latest"])
Index_Sourcetype_Size = namedtuple("Index_Sourcetype_Size", ['index', 'sourcetype', 'size'])
Stack_Server =  namedtuple("Stack_Server", ['stack', 'server'])

class SAIAFieldSummaryModularInputConfig:
    """
    A class responsible for managing the Security Assistant mod_input configuration.
    """

    def __init__(self, session_key: str, this_logger: logging.Logger = logger):
        self.session_key = session_key
        self.logger = this_logger
        config_data = self._retrieve_sa_config_data()
        config = self._parse_sa_config_data(config_data)
        self.settings = config["settings"]
        self.status = config["status"]
        self.search_params = config["search_params"]

    def _retrieve_sa_config_data(self) -> Dict:
        self.logger.debug(f"Fetching SA configuration data from {SA_CONF_URL}")
        return send_request(
            url=SA_CONF_URL,
            method="get",
            headers={
                "Authorization": f"Splunk {self.session_key}",
                "Accept": "application/json",
            },
            output_mode=OutputMode.PARSE_JSON,
        )

    def _update_sa_config_data(self) -> Dict:
        self.logger.debug(f"Updating SA configuration data at {SA_CONF_URL}/status")
        return send_request(
            url=f"{SA_CONF_URL}/status",
            method="post",
            data={
                "last_context_update_timestamp": self.status["last_context_update_timestamp"],
            },
            headers={
                "Authorization": f"Splunk {self.session_key}",
                "Accept": "application/json",
            },
            output_mode=OutputMode.PARSE_JSON,
        )

    def _parse_sa_config_data(self, config_data: Dict) -> Dict:
        self.logger.debug("Parsing configuration entries into structured settings and status")
        config = {}
        for entry in config_data["entry"]:
            content = entry["content"]
            if entry["name"] == "settings":
                config["settings"] = {
                    "context_update_interval_seconds": int(content["context_update_interval_seconds"].strip() or 0),
                    "max_search_range_seconds": int(
                        content["max_search_range_seconds"].strip() or DEFAULT_MAX_SEARCH_RANGE_SECONDS
                    )
                    or DEFAULT_MAX_SEARCH_RANGE_SECONDS,
                    "max_chunk_range_seconds": int(
                        content["max_chunk_range_seconds"].strip() or DEFAULT_MAX_CHUNK_RANGE_SECONDS
                    )
                    or DEFAULT_MAX_CHUNK_RANGE_SECONDS,
                }
            elif entry["name"] == "status":
                config["status"] = {
                    "last_context_update_timestamp": int(content["last_context_update_timestamp"].strip() or 0),
                }
            elif entry["name"] == "search_params":
                config["search_params"] = {
                    "min_minimum_event_count_per_tuple": int(content["min_minimum_event_count_per_tuple"].strip() or D_MIN_MINIMUM_EVENT_COUNT_PER_TUPLE),
                    "max_minimum_event_count_per_tuple": int(content["max_minimum_event_count_per_tuple"].strip() or D_MAX_MINIMUM_EVENT_COUNT_PER_TUPLE),
                    "maximum_event_batch_size": int(content["maximum_event_batch_size"].strip() or D_MAXIMUM_EVENT_BATCH_SIZE),
                    "target_search_count": int(content["target_search_count"].strip() or D_TARGET_SEARCH_COUNT),
                    "maximum_search_count": int(content["maximum_search_count"].strip() or D_MAXIMUM_SEARCH_COUNT),
                    "maximum_tuples_for_search": int(content["maximum_tuples_for_search"].strip() or D_MAXIMUM_TUPLES_FOR_SEARCH),
                    "maximum_count_no_sampling": int(content["maximum_count_no_sampling"].strip() or D_MAXIMUM_COUNT_NO_SAMPLING),
                    "maximum_events_search_count":  int(content["maximum_events_search_count"].strip() or D_MAXIMUM_EVENTS_SEARCH_COUNT)
                }

        return config

    def update(
        self,
        *,
        last_context_update_timestamp: Optional[int] = None,
    ) -> None:
        if last_context_update_timestamp is not None:
            self.status["last_context_update_timestamp"] = last_context_update_timestamp
        try:
            self._update_sa_config_data()
        except Exception as e:
            self.logger.exception("Failed to update SA configuration data: %s", e)

    def get_search_params(self) -> dict:
        return self.search_params

    def is_context_update_due(self) -> bool:
        return (
            int(time.time()) - self.status["last_context_update_timestamp"]
            >= self.settings["context_update_interval_seconds"]
        )

    def __str__(self):
        return f"settings={self.settings}, status={self.status}, search_params={self.search_params}"



class SAIAFieldSummaryModularInput(JsonModularInput):
    def __init__(self, this_logger: logging.Logger = logger):
        environment.app_root = os.path.join(os.path.dirname(__file__), "..")
        scheme_args = {
            "title": "Splunk AI Assistant for SPL Field Summary Modular Input",
            "description": "Collects field summary context for personalized SPL generation",
            "use_external_validation": "true",
            "streaming_mode": "json",
            "use_single_instance": "true",
            "supported_on_cloud": "true",
            "supported_on_onprem": "true",
            "supported_on_fedramp": "false",
            # "skip_wait_for_es": "true", # Doesn't matter, we're not ES
        }
        self._chunk_count = 0
        self.search_params = dict()
        args = [BooleanField("debug", "Debug", "If true, debug logging is enabled.", required_on_create=False)]
        super().__init__(scheme_args, args, this_logger)



    def create_search(self, spl: str, time_modifiers: TimeModifiers, spl_name: str, sample_ratio=None, max_time=None) -> str:
        """
        creates a splunk search and returns the sid of the search
        :param spl: str -> spl string to generate a search fo
        :param time_modifiers: TimeModifers -> TimeModifiers tuple which contains earliest and latest times for the search
        :param spl_name: str -> name of the spl to log
        :param sample_ratio: int -> species how many samples should we sample
        :return sid: str -> a list of search results
        """

        self.logger.debug(f"Creating search job for '{spl_name}'")
        try:
            sid = create_search_job(
                search=spl,
                earliest=time_modifiers.earliest,
                latest=time_modifiers.latest,
                sample_ratio=sample_ratio,
                max_time=max_time,
                session_key=self._input_config.session_key,
                logger=self.logger,
            )
            if not sid:
                raise Exception(f"Failed to create search job for '{spl_name}' SPL context")
            return sid
        except SplunkdConnectionException as e:
            raise Exception(f"Failed to create search job for '{spl_name}' SPL context") from e


    def create_search_and_get_results(self, spl: str, time_modifiers: TimeModifiers, spl_name: str, sample_ratio=None, max_time=None) -> list:
        """
          creates a splunk search and returns the results of the search
          :param spl: str -> spl string to generate a search fo
          :param time_modifiers: TimeModifers -> TimeModifiers tuple which contains earliest and latest times for the search
          :param spl_name: str -> name of the spl to log
          :param sample_ratio: int -> species how many samples should we sample
          :return content: list[dict[str,str]] -> a list of search results
        """

        try:
            sid = self.create_search(spl=spl, time_modifiers=time_modifiers,
                                     spl_name=spl_name, sample_ratio=sample_ratio, max_time=None)
            self.logger.debug(f"Fetching search result for '{spl_name}' SPL context")

            content = get_search_results(
                sid=sid,
                session_key=self._input_config.session_key,
                logger=self.logger,
                strict_unicode=False,
            )
            if content is None:
                self.logger.warning(f"Failed to fetch search result for '{spl_name}' SPL context")
                return []
            return content
        except (SplunkdConnectionException, ResourceNotFound) as e:
            raise Exception(f"Failed to fetch search result for '{spl_name}' SPL context") from e

    # TODO: Can we share code below somewhere between MC and SAIA?
    def get_spl_context(self, *, earliest: int, latest: int) -> Dict[str, Dict]:
        """
        runs several searches to gather splunk environement context
        :param earliest: int-> the earliest time to run all the searches for
        :param latest: int -> the latest time to run all the searches for
        :return spl_context dict[str,Dict]] -> a dictionary containing the context for each of the types
        """
        spl_context = {}

        # process field summaries spl context differently
        field_summaries_content = self.get_field_summaries_spl_context(earliest=earliest, latest=latest)

        spl_context[FIELD_SUMMARIES_CONTEXT_NAME] = field_summaries_content

        return spl_context
    def get_field_summaries_spl_context(self, *, earliest: int, latest: int):
        """
        runs several searches to gather splunk environement context
        :param earliest: int-> the earliest time to run all the searches for
        :param latest: int -> the latest time to run all the searches for
        :return field_summaries_context list[dict[str,str]]-> a list of field summary count rows in the following format:
        rows for each index, sourcetype, field, count group where each the columns for the row are
        stackname, splunk_server, index, sourcetype, field, count. The values for stackname and splunk server are only.
        populated in the first row.
        """

        field_summaries_context = []
        index_sourcetype_macro = "spl_index_sourcetype"
        index_sourcetype_size_tuples = list()
        index_sourcetype_size_dict = dict()
        index_sourcetype_total_event_count: int = 0

        time_modifiers = TimeModifiers(
                earliest=time_modifier_from_timestamp(earliest), latest=time_modifier_from_timestamp(latest)
            )
        try:
            self.logger.info("get index sourcetype tuples")
            # get index sourcetype size tuples
            content = self.create_search_and_get_results(spl=f"`{index_sourcetype_macro}`", time_modifiers=time_modifiers,
                                                         spl_name=index_sourcetype_macro, max_time=120)
            try:
                for content_row in content:
                    idx_src_size_tuple = Index_Sourcetype_Size(index=content_row["indexname"], sourcetype=content_row["sourcetypename"], size=int(content_row["size"]))
                    index_sourcetype_size_tuples.append(idx_src_size_tuple)

                    # needed for final context output field, sourcetypesize
                    if not index_sourcetype_size_dict.get(content_row["indexname"], None):
                        index_sourcetype_size_dict[content_row["indexname"]] = dict()

                    index_sourcetype_size_dict[content_row["indexname"]][content_row["sourcetypename"]] = content_row["size"]
                    index_sourcetype_total_event_count += int(content_row["size"])

            except (IndexError, KeyError, ValueError) as e:
                raise Exception(f"Failed to fetch search result for '{index_sourcetype_macro}' SPL context") from e

            self.logger.info("get search batches")

            # do not proceed if index_sourcetype_size_tuples is empty:

            if not index_sourcetype_size_tuples:
                raise Exception("Failed to fetch search result for field summaries SPL context, no index_sourcetype_size tuples")

            # If we exceed the limit for total events scanned over, remove tuples descending by size until we are under the limit
            if index_sourcetype_total_event_count > self.search_params["maximum_events_search_count"]:
                while index_sourcetype_total_event_count > self.search_params["maximum_events_search_count"]:
                    entry = index_sourcetype_size_tuples.pop(0)
                    logger.info(f"Deleting entry index={entry.index} sourcetype={entry.sourcetype} size={entry.size}")
                    index_sourcetype_total_event_count = sum(entry.size for entry in index_sourcetype_size_tuples)

            # create batches which contain each tuple in on of six buckets: 1,10,100,100,10000,100000
            sample_ratio_buckets = self.create_field_summary_batches(index_sourcetype_size_tuples)

            # proceed with the stats command search for each batch, and apply the correct sampling.
            for sample_ratio in sample_ratio_buckets:
                batches_in_bucket = sample_ratio_buckets[sample_ratio]["batches"]
                large_batches_in_bucket = sample_ratio_buckets[sample_ratio]["large_index_sourcetype_size_tuple_list"]
                maximum_event_batch_size = sample_ratio * self.search_params["maximum_event_batch_size"]
                spl_list = list()
                for batch_number, batch in enumerate(batches_in_bucket):
                    sourcetype_fields_count_spl = self.index_sourcetype_fields_count_command(batch)
                    spl_list.append((sourcetype_fields_count_spl, batch_number))
                for batch_number, batch in enumerate(large_batches_in_bucket):
                    sourcetype_fields_count_spl = self.index_sourcetype_fields_count_command(batch,
                                                                                                 head_value=maximum_event_batch_size)
                    spl_list.append((sourcetype_fields_count_spl, f"large {batch_number}"))

                for spl, batch_number in spl_list:
                    # get content for a batch
                    spl_name = f"batch {batch_number}, sample_ratio={sample_ratio}"
                    content = self.create_search_and_get_results(spl=spl,
                                                                 time_modifiers=time_modifiers,
                                                                 spl_name=spl_name,
                                                                 sample_ratio=sample_ratio)
                    #if context is empty list, retry search with next higher sampling ratio, only once
                    if not content:
                        new_sample_ratio = min(sample_ratio * 10, LARGEST_SAMPLING_RATIO)
                        self.logger.info(f"Search produced no results, retrying with sample ratio {new_sample_ratio}")

                        spl_name = f"batch {batch_number}, sample_ratio={new_sample_ratio}"

                        content = self.create_search_and_get_results(spl=spl,
                                                                     time_modifiers=time_modifiers,
                                                                     spl_name=spl_name,
                                                                     sample_ratio=new_sample_ratio)
                    self.transpose_field_summaries_content(content=content, field_summaries_context=field_summaries_context, index_sourcetype_size_dict=index_sourcetype_size_dict, spl_name=spl_name)

        except Exception as e:
            raise e
        self.logger.info(f"Successfully got field_summaries_context")

        return {"results": field_summaries_context}

    @staticmethod
    def transpose_field_summaries_content(content, field_summaries_context, index_sourcetype_size_dict, spl_name):
        """
        transpose the content returned by the field summaries search so it is in the following format:
        rows for each index, sourcetype, field, count group where each the columns for the row are
        stackname, splunk_server, index, sourcetype, field, count. The values for stackname and splunk server are only.
        populated in the first row. We also want to exclude index, sourcetype, field, count rows that contains count values
        of 0.
        :param content: list[dict[str,str]] -> content returned by the field summaries search
        the format of content: each result row contains the following columns: index, sourcetype, all the fields of
        events containing this particular index and sourcetype value. The value of the field columns are the counts of events
        containing this field value.
        stackname, splunk_server, index, sourcetype, field, count. The values for stackname and splunk server are only
        :param field_summaries_context: list[dict[str,str]] -> result content list to modify with transposed value
        :param index_sourcetype_size_dict: dict[dict[str]] -> dictionary that contains the information for sourcetypesize
        :param spl_name: str -> name of the spl to log
        :return : None
        """

        try:
            for data_row in content:
                index = data_row["index"]
                sourcetype = data_row["sourcetype"]
                for field in data_row:
                    if field not in ["index", "sourcetype"]:
                        # only add an index, sourcetype, field, count row if count > 0
                        if int(data_row[field]) > 0:
                            field_name = re.sub(r'^c\(|\)$', '', field)
                            sourcetypesize = ""
                            if index_sourcetype_size_dict.get(index):
                                sourcetypesize = index_sourcetype_size_dict.get(index).get(sourcetype, "")
                            field_summaries_context_row = {"index": index, "sourcetype": sourcetype, "field": field_name,
                                                           "sourcetypesize": sourcetypesize,
                                                           "count": data_row[field]}

                            field_summaries_context.append(field_summaries_context_row)

        except (IndexError, KeyError, ValueError) as e:
            raise Exception(
                f"Failed to fetch search result for '{spl_name}' for field summaries SPL context") from e

    def create_field_summary_batches(self, index_sourcetype_size_tuples):
        """
        runs several searches to gather splunk environement context
        :param index_sourcetype_size_tuples: list[Index_Sourcetype_Size]-> list of tuples containing the index sourcetype and size info
        :return sample ratio buckets dict[str, dict[str,str]]-> a dictionary containing one of size buckets as keys:
        1,10,100,1000,10000,10000. The values are dictionaries containing two keys "batches", and large_index_sourcetype_size_tuple_list", where
        the values of each are a list of Index_Sourcetype_Size tuples
        """

        minimum_event_count_per_tuple = self.search_params["max_minimum_event_count_per_tuple"]
        while True:
            sample_ratio_buckets, total_searches = self.create_field_summary_batches_helper(index_sourcetype_size_tuples, minimum_event_count_per_tuple=minimum_event_count_per_tuple)
            if total_searches > self.search_params["target_search_count"] and minimum_event_count_per_tuple > self.search_params["min_minimum_event_count_per_tuple"]:
                minimum_event_count_per_tuple =  self.search_params["min_minimum_event_count_per_tuple"] + int((minimum_event_count_per_tuple - self.search_params["min_minimum_event_count_per_tuple"]) * 0.5)
            else:
                break

        # for logging
        metadata = {str(sample_ratio): json.dumps(batches_info) for sample_ratio, batches_info in
                    sample_ratio_buckets.items()}
        self.logger.info(
            "Created batches per sample ratio bucket", extra={"metadata": metadata}
        )

        metadata = {search_param_key : str(search_param_value) for search_param_key, search_param_value in self.search_params.items()}
        metadata["minimum_event_count_per_tuple"] = minimum_event_count_per_tuple

        self.logger.info("Parameters used to build batches", extra={"metadata": metadata})

        metadata = {f"search count for sample ratio {sample_ratio}": str(len(batches_info["batches"]) + len(batches_info["large_index_sourcetype_size_tuple_list"])) for sample_ratio, batches_info in
                    sample_ratio_buckets.items()}

        self.logger.info(f"Total number of searches created: {total_searches}", extra={"metadata": metadata})

        return sample_ratio_buckets



    def create_field_summary_batches_helper(self, index_sourcetype_size_tuples, minimum_event_count_per_tuple=D_MAX_MINIMUM_EVENT_COUNT_PER_TUPLE):
        """
        runs several searches to gather splunk environement context
        :param index_sourcetype_size_tuples: list[Index_Sourcetype_Size]-> list of tuples containing the index sourcetype and size info
        :param minimum_event_count_per_tuple: int -> minimum number of events per tuple which influence how much sampling gets applied to searches
        :return sample ratio buckets dict[str, dict[str,str]]-> a dictionary containing one of size buckets as keys:
        1,10,100,1000,10000,10000. The values are dictionaries containing two keys "batches", and large_index_sourcetype_size_tuple_list", where
        the values of each are a list of Index_Sourcetype_Size tuples
        :return total_searches: the total number of searches that will be run
        """

        # create batches
        # Sort each tuple into one of 6 sample size buckets depending on the counts field in the tuple and the minimum event count
        sample_ratios = [100000, 10000, 1000, 100, 10, 1]
        sample_ratio_buckets = {sample_ratio: {"index_sourcetype_size_tuple_list": [], "batches": [],
                                               "large_index_sourcetype_size_tuple_list": []} for sample_ratio in
                                sample_ratios}
        total_searches = 0
        self.logger.info("sorting tuples into sample ratio buckets based on min event count per tuple and size")
        for index_sourcetype_size_tuple in index_sourcetype_size_tuples:
            for sample_ratio in sample_ratios:
                if index_sourcetype_size_tuple.size >= (minimum_event_count_per_tuple * sample_ratio):
                    if sample_ratio == 1 and index_sourcetype_size_tuple.size > self.search_params["maximum_count_no_sampling"]:
                        sample_ratio_buckets[10]["index_sourcetype_size_tuple_list"].append(
                            index_sourcetype_size_tuple)
                        break
                    sample_ratio_buckets[sample_ratio]["index_sourcetype_size_tuple_list"].append(
                        index_sourcetype_size_tuple)
                    break
            # for the case where the size value of the tuple is less that minimum_event_count_per_tuple, we still want to
            # query it with no sampling
            if index_sourcetype_size_tuple.size < minimum_event_count_per_tuple:
                sample_ratio_buckets[1]["index_sourcetype_size_tuple_list"].append(
                    index_sourcetype_size_tuple)

        self.logger.info("sorting tuples by index")
        # Sort each bucket by index, this will cause batches to be better organized by index.
        for sample_ratio in sample_ratio_buckets:
            sample_ratio_buckets[sample_ratio]["index_sourcetype_size_tuple_list"].sort(key=lambda x: x.index)
        self.logger.info("building batches for each bucket")
        # For each bucket, build batches by popping adding tuples until a maximum event batch size is reached
        for sample_ratio in sample_ratio_buckets:
            maximum_event_sample_ratio_batch_size = sample_ratio * self.search_params["maximum_event_batch_size"]
            index_sourcetype_size_tuple_list = sample_ratio_buckets[sample_ratio]["index_sourcetype_size_tuple_list"]

            running_batch_event_count = 0
            current_batch = []
            for index_sourcetype_size_tuple in index_sourcetype_size_tuple_list:
                if index_sourcetype_size_tuple.size > maximum_event_sample_ratio_batch_size:
                    # a single sourcetype has so many events that it would exceed an entire batch for a particular sampling bin
                    # do not add tuple to main list of batches for a bin, run it with its own query as its own batch with | head <max_events_per_batch>.
                    sample_ratio_buckets[sample_ratio]["large_index_sourcetype_size_tuple_list"].append(
                        [index_sourcetype_size_tuple])
                    continue

                running_batch_event_count += index_sourcetype_size_tuple.size
                # if batch would be over maximum event batch size OR the number of tuples would be too large, use a new search (create a new batch)
                if running_batch_event_count > maximum_event_sample_ratio_batch_size or len(current_batch) + 1 > self.search_params["maximum_tuples_for_search"]:
                    # finalize the nonempty batch, and start building the next batch
                    sample_ratio_buckets[sample_ratio]["batches"].append(current_batch)

                    # reset running batch count and create new empty batch list
                    running_batch_event_count = index_sourcetype_size_tuple.size
                    current_batch = [index_sourcetype_size_tuple]
                else:
                    current_batch.append(index_sourcetype_size_tuple)

            # add the final batch for the bucket
            if current_batch:
                sample_ratio_buckets[sample_ratio]["batches"].append(current_batch)
            total_searches += len(sample_ratio_buckets[sample_ratio]["batches"])
            total_searches += len(sample_ratio_buckets[sample_ratio]["large_index_sourcetype_size_tuple_list"])

            sample_ratio_buckets[sample_ratio].pop("index_sourcetype_size_tuple_list")

        if total_searches > self.search_params["maximum_search_count"]:
            self.logger.info("maximum search count reached, popping off random batches")
            num_of_elements_to_remove = total_searches - self.search_params["maximum_search_count"]
            # From the list of proposed batches, pop off random batches until the length of proposed batches is equal to the maximum number of searches.
            all_batches = []
            for sample_ratio in sample_ratio_buckets:
                sample_ratio_batches = [(sample_ratio, batch, "regular") for batch in
                                        sample_ratio_buckets[sample_ratio]["batches"]]
                sample_ratio_large_batches = [(sample_ratio, batch, "large") for batch in
                                        sample_ratio_buckets[sample_ratio]["large_index_sourcetype_size_tuple_list"]]
                sample_ratio_buckets[sample_ratio]["batches"] = []
                sample_ratio_buckets[sample_ratio]["large_index_sourcetype_size_tuple_list"] = []

                all_batches.extend(sample_ratio_batches)
                all_batches.extend(sample_ratio_large_batches)

            random.shuffle(all_batches)

            for index in range(num_of_elements_to_remove):
                all_batches.pop()

            # repopulate batches of each bucket with new list of batches
            for sample_ratio, batch, batch_type in all_batches:
                if batch_type == "regular":
                    sample_ratio_buckets[sample_ratio]["batches"].append(batch)
                if batch_type == "large":
                    sample_ratio_buckets[sample_ratio]["large_index_sourcetype_size_tuple_list"].append(batch)

        # query plan is ready, log number of searches
        total_searches = self.search_params["maximum_search_count"] if total_searches > self.search_params["maximum_search_count"] else total_searches


        return sample_ratio_buckets, total_searches

    @staticmethod
    def index_sourcetype_fields_count_command(index_sourcetype_size_tuples, head_value : Optional[int] = None) -> str:
        """
        returns an spl which returns the counts of unique fields within an index, sourcetype, field tuple.
        :param index_sourcetype_size_tuples: list(Index_Sourcetype_Size) -> list of tuples containing index, sourcetype, size
        :param head_value: int -> the number of events we should use to calculate the field counts
        :return spl: the spl which returns the field count values
        """
        indexes_str_set = set()
        sourcetypes_str_set = set()

        for index_sourcetype_size_tuple in index_sourcetype_size_tuples:
            indexes_str_set.add(f"index=\"{index_sourcetype_size_tuple.index}\"")
            sourcetypes_str_set.add(f"\"{index_sourcetype_size_tuple.sourcetype}\"")

        indexes_str_set = sorted(indexes_str_set)
        sourcetypes_str_set = sorted(sourcetypes_str_set)

        indexes_str = " OR ".join(indexes_str_set)
        sourcetypes_list_str = ", ".join(sourcetypes_str_set)
        sourcetypes_str = f"sourcetype IN ({sourcetypes_list_str})"

        if head_value:
            return f"search {indexes_str} {sourcetypes_str} | head {head_value} | stats c(*) by index, sourcetype"
        return f"search {indexes_str} {sourcetypes_str} | stats c(*) by index, sourcetype"

    def update_spl_context(self, *, earliest: int, latest: int) -> None:
        start = time_modifier_from_timestamp(earliest)
        end = time_modifier_from_timestamp(latest)
        request_id = str(uuid.uuid4())
        run_start = time.time()
        spl_context = self.get_spl_context(earliest=earliest, latest=latest)
        run_end = time.time()
        logger.info(log_kwargs(
            apply_time=round((run_end - run_start), 5),
        ))
        field_data = spl_context[FIELD_SUMMARIES_CONTEXT_NAME]["results"]

        # Create service for SaiaApi object
        user = "splunk-system-user"
        self.logger.info("Connecting to SAIA service")
        service = connect(
            token=self._input_config.session_key,
            handler=handler(timeout=1),
            host="127.0.0.1",
            app="Splunk_AI_Assistant_Cloud",
            owner=user,
            retries=2
        )

        app_version = get_app_version(service)

        self.logger.info(f"Updating SPL context in range {start} - {end}")
        self.logger.info( # pyright: ignore
            log_kwargs(
                request_id=request_id,
                anticipated_entry_count=len(field_data),
                saia_app_version=app_version,
            )
        )

        hashed_user = deterministic_hash(user)
        api = SaiaApi(
            service,
            service,
            user,
            None,
            hashed_user
        )
        api.submit_sourcetype_metadata(
            field_data=field_data,
            request_id=request_id
        )

        # Also cache EC-side (for future UI work around metadata management)
        collection = SaiaFieldSummaryCollection(service)

        self.cache_results_to_kvstore(field_data, collection)

        # TODO: Clean up logging
        self.logger.info(  # pyright: ignore
            log_kwargs(
                message="Index metadata submitted successfully.",
                saia_app_version=app_version,
        ))
        self.logger.info(f"Successfully updated SPL context in range {start} - {end}")


    def run(self, stanza: Sequence[Dict[str, Any]]):
        self.logger.info("Starting Field Summary Modular Input")
        self.logger.setLevel(self.get_log_level(stanza))
        self.logger.debug(f"Input configuration: {self._input_config}, stanza: {stanza}")
        config = SAIAFieldSummaryModularInputConfig(self._input_config.session_key, self.logger)
        self.search_params = config.get_search_params()
        if config.is_context_update_due():
            now = int(time.time())
            # for POC, setting max lookback to be 24 hours ago
            earliest = max(
                config.status["last_context_update_timestamp"], now - config.settings["max_chunk_range_seconds"]
            )
            latest = int(time.time())

            try:
                self.update_spl_context(earliest=earliest, latest=latest)
            except Exception as e:
                self.logger.exception("Error in updating field summary context: %s", e)

            config.update(last_context_update_timestamp=latest)
            earliest = latest
            self._chunk_count += 1

    def cache_results_to_kvstore(self, results, collection):
        # build an object from the results
        index_sourcetype_map = {}
        for result in results:
            key = result["index"] + result["sourcetype"]
            if key in index_sourcetype_map:
                fields_obj = index_sourcetype_map[key]["fields_obj"]
                if result["field"] in fields_obj:
                    # an imperfect total count (will overcount if there is overlapping sampling lookbacks)
                    fields_obj[result["field"]] = fields_obj[result["field"]] + result["count"]
                else:
                    fields_obj[result["field"]] = result["count"]
            else:
                index_sourcetype_map[key] = {
                    "_key": key,
                    "index": result["index"],
                    "sourcetype": result["sourcetype"],
                    "fields_obj": {
                        result["field"]: result["count"]
                    }
                }

        entries_to_save = index_sourcetype_map.values()
        for entry in entries_to_save:
            entry["fields_obj"] = json.dumps(entry["fields_obj"])
        if entries_to_save:
            collection.batch_save(entries_to_save)



if __name__ == '__main__':
    add_log_extra_metadata("tag", LOGGER_METADATA_TAG)
    add_log_extra_metadata("context", "modinput")
    mod_input = SAIAFieldSummaryModularInput()
    mod_input.execute()
