
import requests
import splunk

from enum import Enum
from functools import wraps
from json.decoder import JSONDecodeError
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import ConnectionError, HTTPError, RequestException, RetryError
from typing import Callable, Collection, Dict, List, Union


class OutputMode(Enum):
    NO_CONTENT = 1  # No response is returned (void behavior).
    PARSE_JSON = 2  # The response will be parsed as JSON.
    RETURN_RAW = 3  # The raw response content will be returned as bytes.
    STREAM = 4  # The response content is streamed in chunks. Caller is responsible for reading and processing it.
    STREAM_JSON = 5  # Server is asked to return JSON response. The response content is streamed in chunks.


def retry(
    func=None,
    *,
    retries: int = 5,
    backoff_factor: float = 0.1,
    status_forcelist: Collection[int] = (408, 429, 500, 502, 503, 504),
):
    """
    A decorator to automatically retry HTTP requests using exponential backoff.

    This decorator can be applied directly to a function or used with additional parameters
    to customize the retry behavior.

    Parameters:
    -----------
    func : callable
        The function to be decorated.
    retries : int, optional
        The maximum number of retry attempts. Default is 5.
    backoff_factor : float, optional
        A multiplier for the delay between retry attempts. The delay increases exponentially with the formula
        `backoff_factor * (2 ** (retry_number - 1))`. Default is 0.5.
    status_forcelist : Collection[int], optional
        A collection of HTTP status codes that should trigger a retry. Default is (408, 429, 500, 502, 503, 504).

    Returns:
    --------
    The result of the decorated function after applying retry logic.

    Example:
    --------
    @retry(retries=3, backoff_factor=0.5, status_forcelist=[500, 502])
    def fetch_data(url, session: requests.Session = None):
        response = session.get(url)
        response.raise_for_status()
        return response.json()

    @retry
    def fetch_data_with_default_retry_args(url, session: requests.Session = None):
        response = session.get(url)
        response.raise_for_status()
        return response.json()

    try:
        response = fetch_data(url="https://splunk.com/api/data")
        print("Data fetched successfully:", response)
    except requests.exceptions.RequestException as e:
        print(f"Request failed after retries: {e}")


    Notes:
    ------
    - The function to be decorated must accept a `session` parameter, which will be used to pass the configured
      `requests.Session` object with retry logic.
    - The decorator modifies the `session` parameter in `kwargs` to include retry logic.

    When To Use
    -----------
    Use this decorator if you need a straightforward, reliable, and consistent way to add retry logic to HTTP requests
    using the requests library.

    Advantages Over the Legacy `blueridge.utils.rest_retry.retry` Decorator:

    - Doesn't Assume Hardcoded Logger Position: The legacy decorator expects the logger is always the second argument
      (`args[1]`). This assumption can lead to an IndexError if the logger is not provided or if the function signature
      changes. Such an error would mistakenly trigger the retry logic, causing unnecessary retries and delays even when
      the decorated function itself didn't fail. This would result in unnecessary delays and potentially misleading
      behavior.
    - Consistent Return Behavior: The legacy decorator returns different types depending on the target_service -
      returning just response for "SCS" and a tuple for other services. This inconsistency can cause issues when the
      decorator is applied to different functions with varying return types. This decorator maintains consistent return
      behavior, making it easier to work with across various functions.

    This decorator provides a more robust and predictable solution, avoiding the pitfalls present in the legacy
    implementation.
    """
    max_retries = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=max_retries)

    @wraps(func)
    def _impl(*args, **kwargs):
        local_session = False
        session = kwargs.get("session")
        if session is None:
            session = requests.Session()
            local_session = True
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        if func:
            kwargs["session"] = session
            try:
                return func(*args, **kwargs)
            finally:
                if local_session:
                    session.close()

        def wrapper(*method_args, **method_kwargs):
            function = args[0]
            method_kwargs["session"] = session
            try:
                return function(*method_args, **method_kwargs)
            finally:
                if local_session:
                    session.close()

        return wrapper

    return _impl


@retry
def send_request(
    *,
    url: str,
    method: str,
    headers: Dict,
    json: Union[Dict, List] = None,
    data: Union[Dict, List, str, bytes] = None,
    output_mode: OutputMode = OutputMode.PARSE_JSON,
    response_hook: Callable[[requests.Response], requests.Response] = None,
    object_hook: Callable = None,
    session: requests.Session = None,
    **kwargs,
) -> Union[requests.Response, Dict, bytes, None]:
    """
    Sends an HTTP request to a REST API endpoint and returns the response object.

    Parameters:
    -----------
    method : str
        The HTTP method to use (e.g., 'get', 'post', 'put', 'delete').
    url : str
        The URL of the API endpoint.
    headers : Dict
        HTTP headers to include in the request.
    json: Dict or List, optional
        json to send in the body of the request (for methods like POST or PUT).
    data: Dictionary, list of tuples, bytes or str, optional
        raw data to send in the body of the request (for methods like POST or PUT).
    output_mode : OutputMode, optional
        Specifies how the response should be handled:
        - `OutputMode.PARSE_JSON`: The response will be parsed as JSON.
        - `OutputMode.RETURN_RAW`: The raw response content will be returned as bytes.
        - `OutputMode.STREAM`: The response will be streamed, and the caller is responsible for reading it.
        - `OutputMode.STREAM_JSON`: The JSON response will be streamed, and the caller is responsible for reading it.
        - `OutputMode.NO_CONTENT`: No value will be returned.
    response_hook : Callable, optional
        A custom function to process the received requests.Response.
    object_hook : Callable, optional
        A custom function to process the JSON response when using `OutputMode.PARSE_JSON`.
    session : requests.Session, optional
        A requests session to use for the call.
    **kwargs
        Additional parameters to be passed as query parameters or data in the request.

    Returns:
    --------
    Union[requests.Response, Dict, bytes, None]:
        - If `OutputMode.PARSE_JSON`, returns the parsed JSON response as a dictionary.
        - If `OutputMode.RETURN_RAW`, returns the raw response content as bytes.
        - If `OutputMode.STREAM`, returns the `requests.Response` object for streaming.
        - If `OutputMode.STREAM_JSON`, returns the `requests.Response` object for streaming. Expected JSON response.
        - If `OutputMode.NO_CONTENT`, no value is returned (use for void calls).

    Raises
    ______
    RequestError: Raised if the request encounters connection issues, HTTP errors or retry failures, if the JSON
    response cannot be decoded, or if an invalid output mode is specified.

    Notes:
    ------
    - This function retries the request for HTTP errors handled by the `retry` decorator such as 408 (Request Timeout),
      429 (Too Many Requests), or server errors (500, 502, 503, 504).
    - For `OutputMode.STREAM`, the response content must be handled by the caller.
    """
    if output_mode == OutputMode.PARSE_JSON or output_mode == OutputMode.STREAM_JSON:
        kwargs["output_mode"] = "json"
    try:
        response = session.request(
            method=method,
            url=url,
            headers=headers,
            json=json,
            data=data,
            stream=output_mode in (OutputMode.STREAM, OutputMode.STREAM_JSON),
            verify=(splunk.rest.getWebCertFile() or False),
            params=kwargs,
        )
        response.raise_for_status()
    except RetryError as e:
        raise Exception("Failed call to the endpoint") from e
    except ConnectionError as e:
        raise Exception("Failed to connect to the endpoint") from e
    except HTTPError as e:
        raise Exception("HTTP error occurred", e.response.status_code, e.response.content, e.response.headers) from e
    except RequestException as e:
        raise Exception("Failed to send request") from e
    if response_hook is not None:
        response = response_hook(response)
    # Handle response based on output mode
    if output_mode == OutputMode.STREAM or output_mode == OutputMode.STREAM_JSON:
        return response
    if output_mode == OutputMode.RETURN_RAW:
        return response.content
    if output_mode == OutputMode.PARSE_JSON:
        try:
            return response.json(object_hook=object_hook)
        except JSONDecodeError as e:
            raise Exception("Invalid JSON response from the endpoint") from e
    if output_mode == OutputMode.NO_CONTENT:
        return

    raise Exception("Unexpected output mode")
