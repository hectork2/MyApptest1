
import logging
import logging.config
import logging.handlers
import os.path
import time
import socket
import threading
from collections import OrderedDict
import json
from copy import deepcopy

from splunk.clilib.bundle_paths import make_splunkhome_path

_local_data = threading.local()


def clear_log_extra():
    _local_data.extra = {}


def set_log_extra(key, value):
    if 'extra' not in _local_data.__dict__:
        _local_data.extra = {}
    _local_data.extra[key] = value


def override_log_extra(extra):
    # Used when bootstrapping log extra for multithreading, where new threads need to reset log extra en masse
    _local_data.extra = deepcopy(extra)


def add_log_extra_metadata(key, value):
    if 'extra' not in _local_data.__dict__:
        _local_data.extra = {'metadata': {key: value}}
    else:
        extra = _local_data.extra
        if 'metadata' not in extra:
            extra['metadata'] = {key: value}
        else:
            extra['metadata'][key] = value


class JSONFormatter(logging.Formatter):
    """
    JSONFormatter returns the output in a JSON format rather than a string.

    Scenario 1: Log messages without any additional information.
        Sample:
            from api.utils.log import setup_logger
            logger  = setup_logger("threat_intelligence_upload")

            logger.info("Some message goes here",
                extra={"stack":<stack_name>, "service": <service_name>})
        Output:
            {"filename": "threatlist.py", "levelname": "INFO", "stack": "missioncontrol67", "hostname": null,
            "message": "Entering GET method", "asctime": "2019-02-11T19:04:16Z",
            "service": "threat-intelligence-service"}


    Scenario 2: Log messages with some addtional/custom fields from the service.
        Note: In this case you must create a sub-class of JSONFormatter and declare the additional fields.
        Sample:
            class ThreatIntelJSONFormatter(JSONFormatter):
                extra_fields = ["threat_category", "threat_name"]

            from api.utils.log import setup_logger
            logger  = setup_logger("threat_intelligence_upload", ThreatIntelJSONFormatter())

            logger.info(
                "Some message",
                extra={"stack":<stack_name>, "service": <service_name>, "threat_category"="something",
                "threat_name"="something"})
        Output:
            {"filename": "threatlist.py", "levelname": "INFO", "stack": "missioncontrol67", "hostname": null,
            "message": "Entering GET method", "asctime": "2019-02-11T19:04:16Z", "
            service": "threat-intelligence-service", "threat_category"="something", "threat_name"="something"}
    """

    converter = time.gmtime

    # These are the default fields that are provided by the logging framework.
    _default_fields = ["message", "asctime", "levelname", "filename", "hostname"]

    # These are the required extra fields that need to be added to the extra param of logger. (stack no longer included)
    _required_extra_fields = ["service"]

    # Any extra/custom fields that need to be added by services.
    extra_fields = []

    def __init__(self, datefmt=None):
        super().__init__(None, datefmt)
        self.record_fields = set(self._default_fields + self._required_extra_fields + self.extra_fields)

    def format(self, record):
        """
        Format the specified record as json.

        The record's attribute dictionary is used as the operand to a string formatting operation which yields the
        returned string. Before formatting the dictionary, a couple of preparatory steps are carried out. The message
        attribute of the record is computed using LogRecord.getMessage(). If the formatting string uses the
        time (as determined by a call to usesTime(), formatTime() is called to format the event time. If there is
        exception information, it is formatted using formatException() and appended to the message.
        """
        output = []
        record.message = record.getMessage()
        record.hostname = socket.gethostname()
        output.append(('location', record.filename + ':' + str(record.lineno)))
        # Always add the asctime to the json log.
        record.asctime = self.formatTime(record, self.datefmt)

        extra = getattr(_local_data, 'extra', None)
        record_extra_defaults = extra if isinstance(extra, dict) else {}

        if record.exc_info:
            record.exc_text = super().formatException(record.exc_info)
            output.append(('callstack', record.exc_text))

        # For POST requests, if the caller is passing payload for debug purposes
        if hasattr(record, 'postPayload'):
            output.append(('postPayload', record.postPayload))

        # If requestPath is passed by caller for debug purpose
        if hasattr(record, 'requestPath'):
            output.append(('requestPath', record.requestPath))
        elif record_extra_defaults.get('requestPath'):
            output.append(('requestPath', record_extra_defaults.get('requestPath')))

        try:
            # Go through each of the record_fields to be included in the JSON and add the value.
            for fld in self.record_fields:
                default_ = record_extra_defaults.get(fld) if fld in self._required_extra_fields else None
                value = record.__dict__.get(fld, default_)

                if fld == 'asctime':
                    fld = 'time'
                if fld == 'levelname':
                    fld = 'level'

                output.append((fld, value))

            metadata = record.metadata if hasattr(record, 'metadata') else {}
            default_metadata = record_extra_defaults.get('metadata', {})
            for key in default_metadata:
                metadata.setdefault(key, default_metadata[key])

            output.append(('metadata', metadata))
            output = OrderedDict(output)
        except UnicodeDecodeError as e:
            raise e

        # We do not really handle this for now. Will make changes here if needed.
        """
        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            if s[-1:] != "\n":
                s = s + "\n"
            try:
                s = s + record.exc_text
            except UnicodeError:
                # Sometimes filenames have non-ASCII chars, which can lead
                # to errors when s is Unicode and record.exc_text is str
                # See issue 8924.
                # We also use replace for when there are multiple
                # encodings, e.g. UTF-8 for the filesystem and latin-1
                # for a script. See issue 13232.
                s = s + record.exc_text.decode(sys.getfilesystemencoding(),
                                               'replace')
        """
        return json.dumps(output)

    def formatTime(self, record, datefmt=None):
        """
        Format the LogRecord to return the formatted asctime in RFC 3339 format. This is called by the format() method
        when asctime is present in the format string.
        """
        created_time = record.created
        tpl = self.converter(created_time)

        if datefmt:
            return time.strftime(datefmt, tpl)

        datefmt = "%Y-%m-%dT%H:%M:%SZ"
        return time.strftime(datefmt, tpl)


def setup_logger(name, formatter=JSONFormatter(), level=logging.INFO, max_bytes=25000000, backup_count=5):
    """
    Setup logger.
    Args:
        name (str): The logger name
        formatter (JSONFormatter): The json formatter instance used to format logs to push to SCP splunk
        level (int): logging level
        max_bytes (int): Maximum bytes after which the log file should rotate
        backup_count (int): How many files should be retained after rotating over
    Returns:
        logger
    """
    if not isinstance(name, str):
        return logging.getLogger()

    logfile = get_splunk_logfile_path()
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False  # Prevent the log messages from being duplicated in the python.log file

    # Prevent re-adding handlers to the logger object, which can cause duplicate log lines.
    handler_exists = any(
        True for h in logger.handlers if isinstance(h, logging.FileHandler) and h.baseFilename == logfile
    )
    if not handler_exists:
        file_handler = logging.handlers.RotatingFileHandler(
            logfile, maxBytes=max_bytes, backupCount=backup_count, delay=True
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def get_splunk_logfile_path():
    """
    Returns the path to the splunk log file
    """
    return make_splunkhome_path(["var", "log", "splunk", "splunk_ai_assistant.log"])


class LogType(type):
    """
    A type class that setups a logger when initializing a class, if the class
    does not define a logger.

    Logger name resolution:
    1. If `logger_name` is defined, create a logger with name `logger_name`.
    2. Otherwise, use module name of the class
    3. If the module name is '__main__', use class name instead.
    """

    def __init__(cls, name, bases, dct):
        super(LogType, cls).__init__(name, bases, dct)

        # explicitly get logger from class dictionary to IDE can "see" that it is set
        # (this is actually redundant)
        cls.logger = dct.get('logger')
        if cls.logger is None:
            logger_name = dct.get('logger_name')
            if logger_name is None:
                logger_name = dct['__module__'].split(".")[-1]
                if logger_name == '__main__':
                    logger_name = name
            cls.logger = setup_logger(name=logger_name, level=logging.INFO)
