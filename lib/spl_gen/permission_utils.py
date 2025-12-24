from splunklib.results import JSONResultsReader, Message
import concurrent.futures
import time
import json
from threading import Thread

from .utils import read_splk_content

# I also see that | dbinspect index=* index=_* | dedup index | fields index might be another option
# Under the hood maybe eventcount uses dbinspect... not super sure
INDEXES_SEARCH = "| eventcount summarize=false `saias_field_summary_indexes` | dedup index | fields index"
# On PT, this takes 2.4 seconds to run, for last 7 days timeframe
SOURCETYPES_SEARCH = "| metadata type=sourcetypes `saias_field_summary_indexes` | dedup sourcetype | fields sourcetype"
TTL = 3600 # 1 hour before permissions obj needs re-fetching
# Not if TTL should be more or less often (could use up search quota)

def build_allowed_array(service, search, field_name):
    job = service.search(
        query=search,
        earliest_time="-7d",
        latest_time="now"
    )

    while not job.is_done():
        time.sleep(.1)
    results = JSONResultsReader(job.results(output_mode='json'))

    res = []
    for result in results:
        if isinstance(result, dict):
            res.append(result[field_name])
        elif isinstance(result, Message):
            pass # TODO: Log informational Messages?

    return res

def get_user_capabilities(service):
    capabilities_res = service.get("/services/authentication/current-context", output_mode="json")
    parsed_result = read_splk_content(capabilities_res)
    return parsed_result.get('capabilities')

def get_user_roles(service):
    roles_res = service.get("/services/authentication/current-context", output_mode="json")
    parsed_result = read_splk_content(roles_res)
    return parsed_result.get('roles')

def run_permission_searches(service):
    indexes = build_allowed_array(service, INDEXES_SEARCH, "index")
    sourcetypes = build_allowed_array(service, SOURCETYPES_SEARCH, "sourcetype")

    roles = get_user_roles(service)
    perms_obj = {
        "indexes": indexes,
        "roles": roles,
        "sourcetypes": sourcetypes,
        "created": time.time()
    }
    return perms_obj

def update_perms_obj_async(key, username, collection, service):
    perms_obj = run_permission_searches(service)
    collection.data.update(key, {
        "saia_user": username, "allowed_obj": perms_obj
    })

def create_perms_obj_async(username, collection, service):
    perms_obj = run_permission_searches(service)
    collection.data.insert(
        {"saia_user": username, "allowed_obj": perms_obj}
    )

def build_permissions_obj(service, username, logger):
    collection =  service.kvstore["saia_allowed_data"]

    results = collection.data.query(
                query={"saia_user": username}
            )

    if len(results) == 0:
        logger.warning("Warning: No permissions found... ")
        # Run search, insert perms object
        perms_obj = {
            "indexes": [],
            "roles": [],
            "sourcetypes": [],
            "created": time.time(),
            "user_id": username
        }
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(run_permission_searches, service)
            try:
                # Wait for the result within the timeout
                perms_obj = future.result(timeout=3)
            except concurrent.futures.TimeoutError:
                # Return an empty perms_obj, trigger async perms_obj creation
                logger.warning("Warning: Permissions search timed out after 10s, returning empty obj and trigggering async update")
                perms_obj = {
                    "indexes": [],
                    "roles": [],
                    "sourcetypes": [],
                    "created": time.time(),
                    "user_id": username
                }
                
                kwargs = {
                    "username": username,
                    "collection": collection,
                    "service": service
                }
                logger.info("Starting async thread to create permissions object")
                thread = Thread(target=create_perms_obj_async, kwargs=kwargs)
                thread.start()
                logger.info("Started async thread to create permissions object")
                return perms_obj

        collection.data.insert(
            {"saia_user": username, "allowed_obj": perms_obj}
        )
        perms_obj["user_id"] = username
        return perms_obj
    else:
        now = time.time()
        # Check expiry, if past expiry fetch again else return cached
        created = results[0]["allowed_obj"]["created"]
        current_obj = results[0]["allowed_obj"]
        if (now - created) > TTL:

            current_obj["created"] = time.time() # Set time to prevent subsequent
            # expired calls from fetching as well
            collection.data.update(results[0]["_key"], {
                "saia_user": username,
                "allowed_obj": current_obj
            })
            kwargs = {
                "key": results[0]["_key"],
                "username": username,
                "service": service,
                "collection": collection
            }
            logger.info("Starting async thread to update permissions object")
            thread = Thread(target=update_perms_obj_async, kwargs=kwargs)
            thread.start()
            logger.info("Started async thread to update permissions object")
        current_obj["user_id"] = username
        return current_obj
