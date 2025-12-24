"""
This module provides functionality to create and run search jobs.
"""
import json
import logging
import time
from splunk import rest, SplunkdConnectionException
from typing import Optional, Mapping, Sequence, Tuple, Union
from requests import codes
from .exceptions import MacroNotFoundOrInvalid, SavedSearchNotFoundOrInvalid

MAX_EVENT_COUNT = 10000

logger = logging.Logger("SAIAModularInputTriggeredSplunkSearch")

""" Possible Search Dispatch States:
QUEUED, PARSING, RUNNING, FINALIZING, PAUSE, INTERNAL_CANCEL,
USER_CANCEL, BAD_INPUT_CANCEL, QUIT, FAILED, DONE.
"""
JOB_DONE = "DONE"
INVALID_DISPATCH_STATES = ["INTERNAL_CANCEL", "USER_CANCEL", "BAD_INPUT_CANCEL", "QUIT", "FAILED"]


def create_search_job(search, earliest, latest, session_key, logger, max_time=None, adhoc_search_level="smart", sample_ratio=None):
    '''
    Create Splunk search job and return sid for the search job.
    '''
    if max_time is None:
        max_time = 30

    if (earliest or earliest == 0) and latest and latest >= earliest:
        postargs = {
            "search": search,
            "max_time": max_time,
            "output_mode": "json",
            "earliest_time": earliest,
            "latest_time": latest,
            "adhoc_search_level": adhoc_search_level,
            "custom.dispatch.sample_ratio" : sample_ratio,
        }
    else:
        postargs = {
            "search": search,
            "max_time": max_time,
            "output_mode": "json",
            "adhoc_search_level": adhoc_search_level,
            "custom.dispatch.sample_ratio": sample_ratio,
        }
    if sample_ratio:
        postargs["custom.dispatch.sample_ratio"] = sample_ratio

    logger.info("Creating search job", extra={"metadata": postargs})
    # Create a search job
    resp, content = rest.simpleRequest('/search/jobs', sessionKey=session_key, postargs=postargs)

    if resp.status == 201:
        content = json.loads(content)
        sid = content['sid']
        logger.info("Search Job: sid=%s", sid)
        return sid
    logger.error("Error creating search job. status=%s content=%s", resp.status, content)
    return None


def get_search_results(
    sid,
    session_key,
    logger,
    count=None,
    offset=None,
    wait_time=5,
    return_events=False,
    return_full_response=False,
    search="",
    strict_unicode=True,
):
    '''
    Get the search results from the search run.
    '''
    results = None

    if count is None:
        count = MAX_EVENT_COUNT
    count = min(count, MAX_EVENT_COUNT)
    if offset is None:
        offset = 0

    getargs = {"output_mode": "json", "count": count, "offset": offset}
    postargs = {"output_mode": "json", "count": count, "offset": offset, "search": search}

    while True:
        resp, content = rest.simpleRequest(f'/search/v2/jobs/{sid}', sessionKey=session_key, getargs=getargs)

        if resp.status == 200:
            content = json.loads(content)
            content = content['entry'][0]['content']
            status = content["dispatchState"]

            # If the search is complete with results.
            if status == JOB_DONE:
                logger.info('Completed running search. Getting results.')

                if return_events:
                    result_url = f'/search/v2/jobs/{sid}/events'
                else:
                    result_url = f'/search/v2/jobs/{sid}/results'
                resp, content = rest.simpleRequest(result_url, sessionKey=session_key, postargs=postargs, method="POST")
                if resp.status == 200:
                    try:
                        content = json.loads(content)
                    except UnicodeDecodeError as e:
                        if strict_unicode:
                            raise
                        logger.exception("failed to decode search results: %s", e)
                        content = json.loads(content.decode("utf-8", "backslashreplace").encode('unicode-escape'))

                    # results is a list of dictionary. can be empty but results field will always be in the response
                    # https://docs.splunk.com/Documentation/Splunk/9.0.2/RESTREF/RESTsearch#search.2Fjobs.2F.7Bsearch_id.7D.2Fresults_.28deprecated.29
                    if return_full_response:
                        results = content
                    else:
                        results = content["results"]
                        if len(results) == 0:
                            logger.error(f"got empty search results: {content}")

                    break
                logger.error('Error getting search results. status=%s content=%s', resp.status, content)
                break

            # If the search failed, break.
            elif status in INVALID_DISPATCH_STATES:
                logger.error('Search failed status=%s content=%s', resp.status, content)
                break
            # Wait and retry.
            else:
                # To avoid making multiple calls to the /search/jobs/{sid} endpoint,
                # we wait and re-check if the search job is DONE.
                time.sleep(wait_time)
                logger.info('Waiting for search to complete. dispatchState=%s', status)
        else:
            break
    return results


def run_search_query(
    search: str,
    *,
    session_key: str,
    earliest_time: str = "-1@d",
    latest_time: str = "+0s",
    logger: logging.Logger,
    **kwargs,
) -> Optional[Union[Mapping, Sequence]]:
    logger.debug(f"Running search query: '{search}'")
    try:
        sid = create_search_job(
            search=search,
            earliest=earliest_time,
            latest=latest_time,
            session_key=session_key,
            logger=logger,
            max_time=kwargs.pop("max_time", None),
            adhoc_search_level=kwargs.pop("adhoc_search_level", "smart"),
        )
        if sid:
            return get_search_results(sid=sid, session_key=session_key, logger=logger, **kwargs)
    except (SplunkdConnectionException, TypeError) as e:
        logger.error("Failed to run search query '{search}': %r", e)
    return None


def get_job(sid, session_key):
    resp, content = rest.simpleRequest(
        f'/search/jobs/{sid}',
        sessionKey=session_key,
        getargs={
            "output_mode": "json",
        },
    )
    return resp.status, json.loads(content)


def is_job_complete(sid, session_key):
    status, content = get_job(sid, session_key)
    if status == 200:
        content = content['entry'][0]['content']
        status = content["dispatchState"]

        ## If the search is complete with results.
        if status == "DONE":
            return True
    return False


def touch_job(sid, session_key):
    resp, _ = rest.simpleRequest(
        f'/search/jobs/{sid}/control',
        sessionKey=session_key,
        postargs={
            "action": "touch",
        },
    )
    return resp.status


def validate_spl(spl, session_key):
    resp, content = rest.simpleRequest(
        '/search/v2/parser', sessionKey=session_key, postargs={"q": spl, "output_mode": "json"}
    )
    if resp.status == 200:
        # Valid
        return True, None
    if resp.status == 400:
        # Invalid
        return False, str(content)

    logger.error("Unexpected error validating SPL", extra={"metadata": {"parse_response": str(content)}})
    return False, "Unknown Error"


def get_job_progress(sid, session_key) -> Tuple[int, Optional[str], Optional[dict]]:
    status, job_content = get_job(sid, session_key)
    if status == 200:
        content = job_content['entry'][0]['content']
        dispatch_status = content.get("dispatchState")
        return status, dispatch_status, job_content

    return status, None, None


def wait_for_job_content_progress_done(sid, session_key, wait_time=2):
    '''
    different from is_job_complete in that we check if the doneProgress for the search job is 1, which can
    be true even if the job is not complete. We wait until doneProgress is 1
    return:
        status: status of the search/{sid}/results api call
        content: content of the search/{sid}/results api call if status is 200 and doneProgress == 1. Else it is None
    '''
    while True:
        status, dispatch_status, job_content = get_job_progress(sid, session_key)
        if status == 200:
            if dispatch_status == JOB_DONE:
                return status, job_content
            if dispatch_status in INVALID_DISPATCH_STATES:
                return status, None

            logger.info(
                "Waiting for search to complete",
                extra={"metadata": {"dispatch_status": dispatch_status, "sid": sid}},
            )
            time.sleep(wait_time)
        else:
            # Status not 200
            return status, None


def get_search_macro_definition(session_key, macro_name):
    response, content = rest.simpleRequest(
        f'/servicesNS/nobody/Splunk_AI_Assistant_Cloud/admin/macros/{macro_name}',
        method='GET',
        sessionKey=session_key,
        getargs={'output_mode': 'json'},
    )

    if not response.status == codes.ok:
        raise MacroNotFoundOrInvalid(macro=macro_name, returned_status=response.status)

    try:
        content = json.loads(content)
        definition = content['entry'][0]['content']['definition']
    except Exception as e:
        raise MacroNotFoundOrInvalid(
            'Could not process macro data', macro=macro_name, returned_status=response.status
        ) from e

    return definition


def get_saved_search_definition(session_key, search_name):
    response, content = rest.simpleRequest(
        f'/servicesNS/nobody/Splunk_AI_Assistant_Cloud/saved/searches/{search_name}',
        method='GET',
        sessionKey=session_key,
        getargs={'output_mode': 'json'},
    )

    if not response.status == codes.ok:
        raise SavedSearchNotFoundOrInvalid(search_name=search_name, returned_status=response.status)

    try:
        content = json.loads(content)
        definition = content['entry'][0]['content']['search']
    except Exception as e:
        raise SavedSearchNotFoundOrInvalid(
            'Could not process saved search data', search_name=search_name, returned_status=response.status
        ) from e

    return definition
