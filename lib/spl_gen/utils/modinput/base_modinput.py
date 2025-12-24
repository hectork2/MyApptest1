
import argparse
import getpass
import hashlib
import json
import logging
import os
import sys
from uuid import uuid4
import time
from urllib.parse import quote
import signal

import splunk
import splunk.auth
import splunk.rest
import splunk.version

from .fields import BooleanField
from .fields import Field
from .fields import FieldValidationException
from .fields import IntervalField
from splunk.util import normalizeBoolean
from .. import pooling
from splunk.clilib.bundle_paths import make_splunkhome_path
from pathlib import Path
from .util import update_mod_input, DISABLE_MOD_INPUT_ACTION
from ..log import setup_logger, add_log_extra_metadata
from ..rest_server.lock import KvObjectLocker, GroupLock
from ...constants import INFRA_CONF_ENDPOINT, MODINPUT_NAMESPACE, WINDOWS_SYS_PLATFORM

is_windows = sys.platform in WINDOWS_SYS_PLATFORM

# Define logger using the name of the script here, versus in the modular_input class.
logger = setup_logger(name='python_modular_input', level=logging.INFO)

kv_lock = KvObjectLocker(fail_without_lock=True, use_file_locks=True, file_namespace=MODINPUT_NAMESPACE, wait_time=1)


class ModularInputConfig:
    def __init__(self, server_host, server_uri, session_key, checkpoint_dir, configuration):
        self.server_host = server_host
        self.server_uri = server_uri
        self.session_key = session_key
        self.checkpoint_dir = checkpoint_dir
        self.configuration = configuration

    def __str__(self):
        attrs = ['server_host', 'server_uri', 'session_key', 'checkpoint_dir', 'configuration']
        return str({attr: str(getattr(self, attr)) for attr in attrs})

    @staticmethod
    def get_config_from_json(config_str_json):
        """
        Get the config from the given JSON and return a ModularInputConfig instance.

        Arguments:
        config_str_json -- A string of JSON that represents the configuration provided by Splunk.
        """
        configuration = {}
        doc = json.loads(config_str_json)
        server_host = doc.get('server_host')
        server_uri = doc.get('server_uri')
        session_key = doc.get('session_key')
        checkpoint_dir = doc.get('checkpoint_dir')
        conf = doc.get('configuration')

        if isinstance(conf, dict):
            for stanza_name, stanza in conf.items():
                config = {'name': stanza_name, '_app': stanza.get('app')}
                config.update(stanza.get('settings', {}))
                configuration[stanza_name] = config

        return ModularInputConfig(server_host, server_uri, session_key, checkpoint_dir, configuration)


# pylint: disable=too-many-public-methods
class BaseModularInput:

    # These arguments cover the standard fields that are always supplied
    standard_args = [
        BooleanField("disabled", "Disabled", "Whether the modular input is disabled or not"),
        Field("host", "Host", "The host that is running the input"),
        Field("index", "Index", "The index that data should be sent to"),
        IntervalField("interval", "Interval", "The interval the script will be run on"),
        Field("name", "Stanza name", "The name of the stanza for this modular input"),
        Field("source", "Source", "The source for events created by this modular input"),
        Field("sourcetype", "Stanza name", "The name of the stanza for this modular input"),
    ]

    checkpoint_dir = None

    # pylint: disable=too-many-branches
    def __init__(self, scheme_args, args=None, this_logger=None):
        """
        Set up the modular input.

        Arguments:
        scheme_args -- The title (e.g. "Database Connector"), description of the input (e.g. "Get data from a database"), etc.
        args -- A list of Field instances for validating the arguments
        this_logger - A logger instance (defaults to None)
        """
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)

        # default to global logger (python_modular_input)
        self.logger = this_logger if this_logger is not None else logger
        # TODO: Need to refactor SAIA-side logging to do something like this
        add_log_extra_metadata('pid', os.getpid())
        add_log_extra_metadata('request_id', str(uuid4()))

        self.WAIT_TIME = 5

        # Directory to save data that can be shared across modular inputs.
        self.shared_dir = "shared_data"

        # Determine if this is a cloud instance.
        self.is_cloud_stack = None

        # Set the scheme arguments.
        for arg in ['title', 'description', 'use_external_validation', 'streaming_mode', 'use_single_instance']:
            setattr(self, arg, self._is_valid_param(arg, scheme_args.get(arg)))

        for arg in ['always_run', 'requires_kvstore', 'is_continuous_mod_input']:
            if arg in scheme_args and normalizeBoolean(scheme_args[arg]) is True:
                setattr(self, arg, True)
            else:
                setattr(self, arg, False)

        # Settings for supported_on_cloud, supported_on_onprem and supported_on_fedramp
        # If these values are not found, we set both to True and execute the mod input.
        for arg in ['supported_on_cloud', 'supported_on_onprem', 'supported_on_fedramp']:
            if arg in scheme_args:
                setattr(self, arg, normalizeBoolean(scheme_args[arg]))
            else:
                setattr(self, arg, True)

        for arg in ['kvstore_wait_time']:
            try:
                setattr(self, arg, int(scheme_args[arg]))
            except Exception:
                setattr(self, arg, 0)

        for arg in ['require_configured_app']:
            try:
                setattr(self, arg, scheme_args[arg])
            except Exception:
                setattr(self, arg, None)

        # Set this to True if a mod input needs to run regardless of ES being enabled and configured
        try:
            setattr(self, 'skip_wait_for_es', normalizeBoolean(scheme_args['skip_wait_for_es']))
        except Exception:
            setattr(self, 'skip_wait_for_es', False)

        # run_only_on_captain will ensure that the modular input is executed
        # only on the captain on a SHC stack. It is set to "true" by default.
        try:
            setattr(self, 'run_only_on_captain', normalizeBoolean(scheme_args['run_only_on_captain']))
        except Exception:
            setattr(self, 'run_only_on_captain', True)

        self.args = [] if args is None else args[:]

        # Though unused here, this is used for most of our unit test to ensure
        # the scheme_args are as expected.
        self.scheme_args = scheme_args

    def _is_valid_param(self, name, val):
        """Raise an error if the parameter is None or empty."""
        if val is None:
            raise ValueError(f"The {name} parameter cannot be none")

        if len(val.strip()) == 0:
            raise ValueError(f"The {name} parameter cannot be empty")

        return val

    def addArg(self, arg):
        """
        Add a given argument to the list of arguments.

        Arguments:
        arg -- An instance of Field that represents an argument.
        """

        if self.args is None:
            self.args = []

        self.args.append(arg)

    def do_scheme(self, out=sys.stdout):
        """
        Get the scheme and write it out to standard output.

        Arguments:
        out -- The stream to write the message to (defaults to standard output)
        """

        self.logger.info("Modular input: scheme requested")
        out.write(self.get_scheme())

        return True

    def get_scheme(self):
        """
        Get the scheme of the inputs parameters and return as a string.
        """

        raise Exception("get_scheme function was not implemented")

    def do_validation(self, in_stream=sys.stdin):
        """
        Get the validation data from standard input and attempt to validate it. Returns true if the arguments validated, false otherwise.

        Arguments:
        in_stream -- The stream to get the input from (defaults to standard input)
        """

        data = self.get_validation_data(in_stream)

        try:
            self.validate(data)
            return True
        except FieldValidationException as e:
            self.print_error(str(e))
            return False

    def validate(self, arguments):
        """
        Validate the argument dictionary where each key is a stanza.

        Arguments:
        arguments -- The parameters as a dictionary.
        """

        # Check each stanza
        self.validate_parameters(arguments)
        return True

    def validate_parameters(self, parameters):
        """
        Validate the parameter set for a stanza and returns a dictionary of cleaner parameters.

        Arguments:
        parameters -- The list of parameters
        """

        cleaned_params = {}

        # Append the arguments list such that the standard fields that Splunk provides are included
        all_args = {}

        for a in self.standard_args:
            all_args[a.name] = a

        for a in self.args:
            all_args[a.name] = a

        # Convert and check the parameters
        for name, value in parameters.items():
            # If the argument was found, then validate and convert it
            if name in all_args:
                cleaned_params[name] = None if value == '' else all_args[name].to_python(value)

            # Throw an exception if the argument could not be found
            elif name not in [
                '_app',
                'python.version',
                'run_only_one',
                'start_by_shell',
                'run_only_on_captain',
            ]:  # exclude _app, python.version, run_only_one, start_by_shell
                raise FieldValidationException(f"The parameter '{name}' is not a valid argument")

        return cleaned_params

    def print_error(self, error, out=sys.stdout):
        raise Exception("print_error function was not implemented")

    def read_config(self, in_stream=sys.stdin):
        raise Exception("read_config method was not implemented")

    # pylint: disable=too-many-locals,too-many-return-statements,too-many-statements,too-many-branches
    def do_run(self, in_stream=sys.stdin, log_exception_and_continue=False):
        """
        Read the config from standard input and return the configuration.

        in_stream -- The stream to get the input from (defaults to standard input)
        log_exception_and_continue -- If true, exceptions will not be thrown for invalid configurations and instead the stanza will be skipped.
        """
        # Run the modular import
        input_config = self.read_config(in_stream)

        # Save input config for future use (contains the session key).
        self._input_config = input_config

        # Set the new checkpoint directory path.
        mission_control_local_dir = make_splunkhome_path(["etc", "apps", "Splunk_AI_Assistant_Cloud", "local"])
        if is_windows:
            modinput_path = '\\'.join(input_config.checkpoint_dir.split('\\')[-2:])
        else:
            modinput_path = '/'.join(input_config.checkpoint_dir.split('/')[-2:])

        new_checkpoint_path = os.path.join(mission_control_local_dir, modinput_path)
        Path(new_checkpoint_path).mkdir(parents=True, exist_ok=True)
        self.checkpoint_dir = new_checkpoint_path
        self._input_config.checkpoint_dir = new_checkpoint_path

        # Get the mod input name.
        if is_windows:
            mod_input_name = input_config.checkpoint_dir.split('\\')[-1]
        else:
            mod_input_name = input_config.checkpoint_dir.split('/')[-1]

        # Create the shared checkpoint directory.
        self.shared_checkpoint_dir = self.create_shared_checkpoint_directory()

        # Need to specifically check the SH member (in case of SHC) because run_only_one
        # is not supported OnPrem. This mechanism is also used by ES to make sure that a mod input is run on one SH.
        if self.run_only_on_captain:
            # Run the modular input only on the captain.
            exec_status, exec_status_msg = pooling.should_execute(session_key=self._input_config.session_key)
            self.logger.debug('msg="Execution status: %s"', exec_status_msg)

            if not exec_status:
                self.logger.info(
                    "Skipping mod input on this non-captain instance. run_only_on_captain=%s", self.run_only_on_captain
                )
                return
        else:
            self.logger.info("Running mod input on all instances. run_only_on_captain=%s", self.run_only_on_captain)

        # Is this input single instance mode?
        single_instance = str(getattr(self, "use_single_instance", '')).strip().lower() in ["true", "t", "1"]

        # Get a list of stanzas
        stanzas = self.get_stanzas(input_config, log_exception_and_continue)

        # Check if modinput is supported in Fedramp environments
        if not self.is_mod_input_supported_on_fedramp(self._input_config.session_key, mod_input_name):
            # If the mod input is not supported, modinput stanzas get disabled.
            self.disable_mod_input_stanzas(self._input_config.session_key, stanzas)
            return

        # Check if this is a cloud instance or not.
        self.is_cloud_stack = pooling.is_cloud_instance(self._input_config.session_key)

        # Determine if the overall mod input is supported on this environment.
        if not self.is_mod_input_supported(self.is_cloud_stack):
            # If the mod input is not supported in this environment, we should go ahead and disable the stanzas.
            self.disable_mod_input_stanzas(self._input_config.session_key, stanzas)
            return

        # Now determine if each of the mod input stanzas are supported on this enviroment.
        stanzas_to_execute = []

        for stanza in stanzas:
            stanzas_to_disable = []
            if not self.is_mod_input_stanza_supported(self.is_cloud_stack, stanza):
                stanzas_to_disable.append(stanza)
            else:
                stanzas_to_execute.append(stanza)

            self.disable_mod_input_stanzas(self._input_config.session_key, stanzas_to_disable)

        stanzas = stanzas_to_execute

        # Call the modular input's "run" method on valid stanzas, handling two cases:
        # a. If this script is in single_instance mode=true, we have received
        #    all stanzas in input_config.configuration. In this case,
        #    a duration parameter is not supported. The run() method may
        #    loop indefinitely, but we do not re-execute.
        # b. If this script is in single_instance_mode=false, each stanza
        #    can have a duration parameter (or not) specifying the time
        #    at which the individual stanza will be re-executed.
        #
        # Note: The "duration" parameter emulates the behavior of the "interval"
        # parameter available on Splunk 6.x and higher, and is mainly used by the
        # VMWare application.

        # TODO: A run() method may pass results back for optional processing

        if stanzas or self.always_run:
            # # Check ES enablement and configuration
            # es_app_name = "SplunkEnterpriseSecuritySuite"

            # # if wait for ES (default)
            # if not self.skip_wait_for_es:
            #     app_info = self.get_app_info(es_app_name)
            #     if not app_info:
            #         self.logger.warning('Cannot get app info for "%s"', es_app_name)
            #         return
            #     is_configured = normalizeBoolean(app_info.get('configured', 0))
            #     is_disabled = normalizeBoolean(app_info.get('disabled', 1))

            #     # if ES is either disabled or not configured do not run the mod input
            #     if not is_configured or is_disabled:
            #         self.logger.warning(
            #             'Modular input (%s) did not execute because app "%s" is configured: %s and disabled: %s',
            #             getattr(self, 'title', 'unknown'),
            #             es_app_name,
            #             is_configured,
            #             is_disabled,
            #         )
            #         return

            # else:
            #     # if skip_wait_for_es is True, don't need to wait for ES to be configured
            #     self.logger.info(
            #         'Modular input (%s) is executed because it has skip_wait_for_es set to (%s)',
            #         getattr(self, 'title', 'unknown'),
            #         self.skip_wait_for_es,
            #     )

            # app configured handling
            if self.require_configured_app:
                is_configured = self.is_configured(self.require_configured_app, assume_true_on_error=True)

                if not is_configured:
                    self.logger.info(
                        'Modular input (%s) did not execute because app "%s" is not configured',
                        getattr(self, 'title', 'unknown'),
                        self.require_configured_app,
                    )
                    return

            # kvstore required/wait handling
            if self.requires_kvstore:
                elapsed = 0
                max_time = self.kvstore_wait_time
                kvstore_ready = False
                while elapsed <= max_time:
                    kvstore_ready = self.is_kvstore_ready(assume_true_on_error=True)
                    if kvstore_ready:
                        break
                    sleep_time = min(self.WAIT_TIME, max_time - elapsed + 1)
                    elapsed += sleep_time
                    time.sleep(sleep_time)
                    self.logger.info(
                        "Modular input (%s) sleeping (for %s seconds) because kvstore was not ready",
                        getattr(self, 'title', 'unknown'),
                        sleep_time,
                    )

                if not (kvstore_ready or self.is_kvstore_ready(assume_true_on_error=True)):
                    self.logger.warning(
                        "Modular input (%s) did not execute because kvstore was not ready",
                        getattr(self, 'title', 'unknown'),
                    )
                    # Exit with zero since this most likely represents kvstore initializing
                    return

            try:
                if single_instance:
                    # Run the input across all defined stanzas and exit.
                    with GroupLock([mod_input_name], kv_lock):
                        self.run(stanzas)
                else:
                    if not stanzas:
                        stanza = {}
                        file_lock_name = mod_input_name
                    else:
                        stanza = stanzas[0]
                        # Retrieve the single input stanza.
                        stanza_name = stanza.get("name").split("://")[1]
                        file_lock_name = f"{mod_input_name}_{stanza_name}"

                    with GroupLock([file_lock_name], kv_lock):
                        self.run(stanza)

                # We sleep so that only sequentially requested mod input runs actually execute.
                time.sleep(1)
            except RuntimeError as e:
                self.logger.warning(
                    "Mod input %s is not run. There is another instance already executing. message=%s",
                    mod_input_name,
                    e,
                )
                return
            except Exception as e:
                self.logger.exception(f"Execution of Mod input failed: {str(e)}")
                return

        else:
            # If continuous mod input, then wait for 30 seconds so that
            # mod input is not started continuously.
            if self.is_continuous_mod_input:
                time.sleep(30)
            self.logger.info("No input stanzas defined")
            return

    def run(self, stanza):
        """
        Run the input using the arguments provided.

        Arguments:
        stanza -- The name of the stanza
        """
        raise NotImplementedError("Run function was not implemented")

    def get_stanzas(self, input_config, log_exception_and_continue=False) -> list:
        """Get the stanzas from input_config."""
        # Validate all stanza parameters.
        stanzas = []
        for stanza_name, unclean_stanza in input_config.configuration.items():
            try:
                stanzas.append(self.validate_parameters(unclean_stanza))
            except FieldValidationException as e:
                if log_exception_and_continue:
                    # Discard the invalid stanza.
                    self.logger.error(f"Discarding invalid input stanza '{stanza_name}': {str(e)}")
                else:
                    raise e

        return stanzas

    def is_mod_input_stanza_supported(self, is_cloud_stack, stanza):
        """Determine if a mod input stanza is supported on the environment.
        If the stanza is not supported on the environment, we do not execute it and disable it.
        """
        stanza_supported_on_cloud = normalizeBoolean(stanza.get("stanza_supported_on_cloud", True))
        stanza_supported_on_onprem = normalizeBoolean(stanza.get("stanza_supported_on_onprem", True))

        is_onprem_stack = not is_cloud_stack

        if is_cloud_stack and stanza_supported_on_cloud:
            return True

        if is_onprem_stack and stanza_supported_on_onprem:
            return True

        self.logger.info(
            "Mod input stanza is not supported on this stack. stanza_supoported_on_cloud=%s, stanza_supported_on_onprem=%s, stanza=%s",
            stanza_supported_on_cloud,
            stanza_supported_on_onprem,
            stanza,
        )

        return False

    def is_mod_input_supported(self, is_cloud_stack):
        """Determine if the mod input itself is supported on the environment.
        If it is not supported, then none of the stanzas of this mod input are executed.
        We go ahead and disable this mod input stanzas too.
        """
        is_onprem_stack = not is_cloud_stack

        if is_cloud_stack and self.supported_on_cloud:
            return True

        if is_onprem_stack and self.supported_on_onprem:
            return True

        self.logger.info(
            "Mod input is not supported on this environment. supoported_on_cloud=%s, supported_on_onprem=%s",
            self.supported_on_cloud,
            self.supported_on_onprem,
        )

        return False

    def is_mod_input_supported_on_fedramp(self, session_key, mod_input_name):
        """Determine if the mod input itself is supported on the Fedramp environmemt.
        If it is not supported, then none of the stanzas of this mod input are executed.
        """

        if not self.supported_on_fedramp:
            try:
                _, content = splunk.rest.simpleRequest(
                    INFRA_CONF_ENDPOINT, getargs={"output_mode": "json"}, sessionKey=session_key, raiseAllErrors=True
                )

                content = json.loads(content)["entry"][0]["content"]
                is_fedramp_env = normalizeBoolean(content.get("fedramp_env"))
                fedramp_env_name = content.get("fedramp_env_name")
                if is_fedramp_env:
                    self.logger.info(
                        f"Mod input is not supported on Fedramp environment",
                        extra={
                            "metadata": {
                                "supported_on_fedramp": self.supported_on_fedramp,
                                "is_fedramp_env": is_fedramp_env,
                                "fedramp_env_name": fedramp_env_name,
                                "mod_input_name": mod_input_name,
                            }
                        },
                    )
                    return False
                self.logger.info(
                    f"Mod input supported on non-fedramp environments",
                    extra={
                        "metadata": {
                            "supported_on_fedramp": self.supported_on_fedramp,
                            "is_fedramp_env": is_fedramp_env,
                            "fedramp_env_name": fedramp_env_name,
                            "mod_input_name": mod_input_name,
                        }
                    },
                )
            except Exception:
                self.logger.exception(
                    "Fedramp configuration fetch failed mod_input_name {mod_input_name}"
                )
        else:
            self.logger.info(
                "Mod input support on Fedramp is set to true",
                extra={
                    "metadata": {
                        "supported_on_fedramp": self.supported_on_fedramp,
                        "mod_input_name": mod_input_name,
                    }
                },
            )

        return True

    def disable_mod_input_stanzas(self, session_key, stanzas):
        """Disable mod input stanzas."""
        if not stanzas:
            return

        for stanza in stanzas:
            name = stanza.get("name")
            mod_input, stanza_name = name.split("://")
            self.logger.info("Disabling mod input: %s", name)

            result = update_mod_input(self.logger, session_key, mod_input, stanza_name, DISABLE_MOD_INPUT_ACTION)

            if result:
                self.logger.info("Successfully disabled mod input: %s", name)
            else:
                self.logger.error("Error disabling mod input: %s", name)

    @staticmethod
    def get_log_level(stanzas, debug_param='debug'):
        """
        Get debug setting from stanzas.

        Arguments:
        stanzas -- The list of stanzas being processed by the modinput.
        """

        if stanzas and any(normalizeBoolean(stanza.get(debug_param)) is True for stanza in stanzas):
            return logging.DEBUG

        return logging.INFO

    @staticmethod
    def get_file_path(checkpoint_dir, stanza):
        """
        Get the path to the checkpoint file.

        Arguments:
        checkpoint_dir -- The directory where checkpoints ought to be saved
        stanza -- The stanza of the input being used
        """

        # nosemgrep because this is not being used for cryptography, it is being used to find filepath of partial results
        return os.path.join(checkpoint_dir, hashlib.sha1(stanza).hexdigest() + ".json") # nosemgrep

    @classmethod
    def last_ran(cls, checkpoint_dir, stanza):
        """
        Determines the date that the input was last run (the input denoted by the stanza name).

        Arguments:
        checkpoint_dir -- The directory where checkpoints ought to be saved
        stanza -- The stanza of the input being used
        """

        fp = None

        try:
            # pylint: disable=consider-using-with
            fp = open(cls.get_file_path(checkpoint_dir, stanza))  # noqa
            checkpoint_dict = json.load(fp)

            return checkpoint_dict['last_run']

        finally:
            if fp is not None:
                fp.close()

    @classmethod
    def needs_another_run(cls, checkpoint_dir, stanza, interval, cur_time=None):
        """
        Determines if the given input (denoted by the stanza name) ought to be executed.

        Arguments:
        checkpoint_dir -- The directory where checkpoints ought to be saved
        stanza -- The stanza of the input being used
        interval -- The frequency that the analysis ought to be performed
        cur_time -- The current time (will be automatically determined if not provided)
        """

        try:
            last_ran = cls.last_ran(checkpoint_dir, stanza)

            return cls.is_expired(last_ran, interval, cur_time)

        except IOError:
            # The file likely doesn't exist
            logger.exception("The checkpoint file likely doesn't exist")
            return True
        except ValueError:
            # The file could not be loaded
            logger.exception("The checkpoint file could not be loaded")
            return True
        except Exception as e:
            # Catch all that enforces an extra run
            logger.exception("Unexpected exception caught, enforcing extra run, exception info: %s", e)
            return True

    @classmethod
    def time_to_next_run(cls, checkpoint_dir, stanza, duration):
        """
        Returns the number of seconds as int until the next run of the input is expected.
        Note that a minimum of 1 second is enforced to avoid a python loop of death
        constricting the system in rare checkpoint dir failure scenarios.
        Snake pun entirely intentional (pythons constrict prey to death, like your cpu).

        Arguments:
        checkpoint_dir -- The directory where checkpoints ought to be saved
        stanza -- The stanza of the input being used
        duration -- The frequency that the analysis ought to be performed
        """

        try:
            last_ran = cls.last_ran(checkpoint_dir, stanza)
            last_target_time = last_ran + duration
            time_to_next = last_target_time - time.time()
            time_to_next = max(time_to_next, 1)
            return time_to_next
        except IOError:
            # The file likely doesn't exist
            logger.warning(
                "Could not read checkpoint file for last time run, likely does not exist, if this persists debug input immediately"
            )
            return 1
        except ValueError:
            # The file could not be loaded
            logger.error(
                "Could not read checkpoint file for last time run, if this persists debug input immediately"
            )
            return 1
        except Exception as e:
            logger.exception("Unexpected exception caught, enforcing extra run, exception info: %s", e)
            return 1

    @classmethod
    def save_checkpoint(cls, checkpoint_dir, stanza, last_run):
        """
        Save the checkpoint state.

        Arguments:
        checkpoint_dir -- The directory where checkpoints ought to be saved
        stanza -- The stanza of the input being used
        last_run -- The time when the analysis was last performed
        """

        fp = None

        try:
            # pylint: disable=consider-using-with
            fp = open(cls.get_file_path(checkpoint_dir, stanza), 'w')  # noqa

            d = {'last_run': last_run}

            json.dump(d, fp)

        except Exception:
            logger.exception("Failed to save checkpoint directory")
        finally:
            if fp is not None:
                fp.close()

    @staticmethod
    def is_expired(last_run, interval, cur_time=None):
        """
        Indicates if the last run time is expired based .

        Arguments:
        last_run -- The time that the analysis was last done
        interval -- The interval that the analysis ought to be done (as an integer)
        cur_time -- The current time (will be automatically determined if not provided)
        """

        if cur_time is None:
            cur_time = time.time()

        return (last_run + interval) < cur_time

    def checkpoint_data_exists(self, filename, checkpoint_dir=None):
        """Returns True if a checkpoint file exists with the given filename."""
        checkpoint_dir = checkpoint_dir or self._input_config.checkpoint_dir
        return os.path.isfile(os.path.join(checkpoint_dir, filename))

    def delete_checkpoint_data(self, filename, checkpoint_dir=None, raise_err=False):
        """
        Delete arbitrary checkpoint data.

        Arguments:
        filename -- The name of the file to create in the checkpoint directory.
        checkpoint_dir -- The directory where checkpoints ought to be saved. Should
            be set only if the intent is to read data from the checkpoint directory
            of a different modular input.
        raise_err -- Determines if errors should be raised

        Returns:
        True if file is successfully removed, False otherwise.
        """
        checkpoint_dir = checkpoint_dir or self._input_config.checkpoint_dir
        checkpoint_fpath = os.path.join(checkpoint_dir, filename)
        exists = os.path.exists(checkpoint_fpath)
        try:
            os.unlink(checkpoint_fpath)
            return True
        except Exception as e:
            if raise_err and exists:
                raise e
            self.logger.debug('msg=%s checkpoint_dir="%s" filename="%s"', e, checkpoint_dir, filename)
        return False

    def set_checkpoint_data(self, filename, data, checkpoint_dir=None):
        """
        Save arbitrary checkpoint data as JSON.

        Arguments:
        filename -- The name of the file to create in the checkpoint directory.
        data -- A Python data structure that can be converted to JSON.
        checkpoint_dir -- The directory where checkpoints ought to be saved. Should
            be set only if the intent is to read data from the checkpoint directory
            of a different modular input.

        Returns:
        True if the data is successfully saved, False otherwise.
        Throws:
        IOError if the checkpoint cannot be saved.

        Note: The caller is repsonsible for ensuring that the filename does not
        clash with other uses.
        """
        checkpoint_dir = checkpoint_dir or self._input_config.checkpoint_dir

        try:
            with open(os.path.join(checkpoint_dir, filename), 'w') as fp:  # noqa
                json.dump(data, fp)
                return True
        except IOError:
            self.logger.exception(
                'msg="IOError exception when saving checkpoint data" checkpoint_dir="%s" filename="%s"',
                checkpoint_dir,
                filename,
            )
        except ValueError:
            self.logger.exception(
                'msg="ValueError when saving checkpoint data (perhaps invalid JSON)" checkpoint_dir="%s" filename="%s"',
                checkpoint_dir,
                filename,
            )
        except Exception:
            self.logger.exception(
                'msg="Unknown exception when saving checkpoint data" checkpoint_dir="%s" filename="%s"',
                checkpoint_dir,
                filename,
            )
        return False

    def get_checkpoint_data(self, filename, checkpoint_dir=None, raise_known_exceptions=False):
        """
        Get arbitrary checkpoint data from JSON.

        Arguments:
        filename -- The name of the file to retrieve in the checkpoint directory.
        checkpoint_dir -- The directory where checkpoints ought to be saved. Should
            be set only if the intent is to read data from the checkpoint directory
            of a different modular input.

        Returns:
        data -- A Python data structure converted from JSON.

        Throws:
        IOError or Exception if the checkpoint cannot be read; ValueError for
        malformed data. The caller should check if the file exists if it is
        necessary to distinguish between invalid data versus missing data.
        """
        checkpoint_dir = checkpoint_dir or self._input_config.checkpoint_dir
        checkpoint_path = os.path.join(checkpoint_dir, filename)
        data = None

        try:
            if os.path.isfile(checkpoint_path):
                with open(checkpoint_path, 'r') as fp:  # noqa
                    data = json.load(fp)
        except (IOError, ValueError) as e:
            self.logger.exception(
                'msg="Exception when reading checkpoint data" checkpoint_dir="%s" filename="%s" exception="%s"',
                checkpoint_dir,
                filename,
                e,
            )
            if raise_known_exceptions:
                raise
        except Exception:
            self.logger.exception(
                'msg="Unknown exception when reading checkpoint data" checkpoint_dir="%s" filename="%s"',
                checkpoint_dir,
                filename,
            )
            raise

        return data

    def get_validation_data(self, in_stream=sys.stdin):
        """
        Get the validation data from standard input

        Arguments:
        in_stream -- The stream to get the input from (defaults to standard input)
        """

        raise Exception("get_validation_data function was not implemented")

    def validate_parameters_from_cli(self, argument_array=None):
        """
        Load the arguments from the given array (or from the command-line) and validate them.

        Arguments:
        argument_array -- An array of arguments (will get them from the command-line arguments if none)
        """

        # Get the arguments from the sys.argv if not provided
        if argument_array is None:
            argument_array = sys.argv[1:]

        # This is the list of parameters we will generate
        parameters = {}

        for i, arg in enumerate(self.args):
            if i < len(argument_array):
                parameters[arg.name] = argument_array[i]
            else:
                parameters[arg.name] = None

        # Now that we have simulated the parameters, go ahead and test them
        self.validate_parameters(parameters)

    def _parse_args(self, argv):
        """Parse custom CLI arguments. this method must remain private to avoid conflict with similarly named methods
        in classes that inherit from this class."""

        warning_text = 'WARNING: this parameter is a custom Apps extension for debugging only.'

        parser = argparse.ArgumentParser(description='Modular input parameters')

        mode_args = parser.add_mutually_exclusive_group()
        debug_args = parser.add_argument_group()

        debug_args.add_argument('--username', action="store", default=None, help=f"Splunk username ({warning_text})")
        debug_args.add_argument('--password', action="store", default=None, help=f"Splunk password ({warning_text})")
        debug_args.add_argument(
            '--infile',
            type=argparse.FileType(),
            default=None,
            help=f"Filename containing XML modular input configuration ({warning_text})",
        )

        mode_args.add_argument('--scheme', action="store_true")
        mode_args.add_argument('--validate-arguments', dest='validate', action="store_true")

        return parser.parse_args(argv)

    def signal_handler(self, signum, frame):
        """Handle SIGINT"""
        sys.exit(0)

    # pylint: disable=too-many-branches
    def execute(self, in_stream=sys.stdin, out_stream=sys.stdout):
        """
        Get the arguments that were provided from the command-line and execute the script.

        Arguments:
        in_stream -- The stream to get the input from (defaults to standard input)
        out_stream -- The stream to write the output to (defaults to standard output)
        """
        # Invalid arguments will cause the modular input to return usage here.
        args = self._parse_args(sys.argv[1:])

        try:
            self.logger.debug("Execute called")

            if args.scheme:
                self.do_scheme(out_stream)

            elif args.validate:
                self.logger.info("Modular input: validate arguments called")

                # Exit with a code of -1 if validation failed
                if not self.do_validation():
                    sys.exit(-1)

            else:
                if args.username:
                    if not args.password:
                        try:
                            args.password = getpass.getpass("Splunk password: ")
                        except Exception:
                            self.logger.exception("Modular input: could not retrieve Splunk password.")

                    try:
                        self._alt_session_key = splunk.auth.getSessionKey(args.username, args.password)
                    except Exception:
                        self.logger.exception("Modular input: session key override failed.")

                # If specified, override the data passed on sys.stdin.
                if args.infile:
                    try:
                        self.do_run(args.infile, log_exception_and_continue=True)
                    except IOError:
                        self.logger.exception(
                            "Modular input: modinput configuration could not be read from file %s.", args.infile.name
                        )
                else:
                    try:
                        self.do_run(in_stream, log_exception_and_continue=True)
                    except IOError:
                        self.logger.exception(
                            "Modular input: modinput configuration could not be read from input stream."
                        )

            self.logger.debug("Execution completed.")

        except Exception as e:
            self.logger.exception(f"Execution failed: {str(e)}")

            # Make sure to grab any exceptions so that we can print a valid error message
            self.print_error(str(e), out_stream)

    def gen_checkpoint_filename(self, stanza_name, modinput_name=None):
        """Generate a checkpoint filename for this stanza. Collision detection
        is not performed explicitly, since we don't expect duplicate stanzas.

        Parameters:
        stanza_name - A string representing the stanza name, which is typically
            in the form <modinput_name>://<stanza_name>
        modinput_name - An alternate modular input name. This can be used to
            construct a safe path to the checkpoint directory of a different
            modular input, which is useful in situations where two modular inputs
            are acting in a producer/consumer relationship.

        Returns: The path to the checkpoint file corresponding to the stanza
            and modular input name. The caller is repsonsible for ensuring that
            the path can read/written.
        """
        checkpoint_filename = stanza_name.split('://')[1] if '://' in stanza_name else stanza_name
        if modinput_name:
            return os.path.join(os.path.dirname(self._input_config.checkpoint_dir), modinput_name, checkpoint_filename)
        return os.path.join(self._input_config.checkpoint_dir, checkpoint_filename)

    def get_checkpoint_update_times(self, stanza_names, modinput_name=None):
        """Get the update timestamps for checkpointed files by stanza name.

        Parameters:

        stanza_names - A list of strings representing stanza names.
        modinput_name - A string representing the name of another modular
            input to derive checkpoint file update timstamps for, if this modular
            input is acting as a consumer of the output of another modular input.

        Returns: A list of tuples:
            [(stanza_name, path_to_checkpoint_file, last_updated_timestamp),
             ...
            ]

        """

        output = []
        for stanza_name in stanza_names:
            path = self.gen_checkpoint_filename(stanza_name, modinput_name)
            if os.path.isfile(path):
                try:
                    fstat = os.stat(path)
                    output.append((stanza_name, path, fstat.st_size, int(fstat.st_mtime)))
                except IOError:
                    output.append((stanza_name, path, None, None))
            else:
                output.append((stanza_name, None, None, None))
        return output

    def is_configured(self, app, assume_true_on_error=False):
        try:
            unused_r, c = splunk.rest.simpleRequest(
                f"admin/localapps/{quote(app, safe='')}",
                sessionKey=self._input_config.session_key,
                getargs={'output_mode': 'json'},
                raiseAllErrors=True,
            )

            c = json.loads(c)['entry'][0]['content']
            return normalizeBoolean(c['configured']) is True
        except Exception:
            return assume_true_on_error

    def get_app_info(self, app):
        try:
            unused_r, c = splunk.rest.simpleRequest(
                f"admin/localapps/{quote(app, safe='')}",
                sessionKey=self._input_config.session_key,
                getargs={'output_mode': 'json'},
                raiseAllErrors=True,
            )

            return json.loads(c)['entry'][0]['content']
        except Exception:
            return None

    def is_kvstore_ready(self, assume_true_on_error=False):
        try:
            unused_r, c = splunk.rest.simpleRequest(
                'kvstore/status/status',
                sessionKey=self._input_config.session_key,
                getargs={'output_mode': 'json'},
                raiseAllErrors=True,
            )

            c = json.loads(c)['entry'][0]['content']
            # kvservice
            if c.get('externalKVStore'):
                return c['externalKVStore']['status'] == 'ready'

            # kvstore
            return c['current']['status'] == 'ready'
        except Exception as e:
            self.logger.warning(e)
            return assume_true_on_error

    def create_shared_checkpoint_directory(self):
        """Create a directory where shared data across the modular inputs can be stored
        and accessed.
        """
        shared_dir_path = make_splunkhome_path(["etc", "apps", "Splunk_AI_Assistant_Cloud", "local", "modinputs", self.shared_dir])
        os.makedirs(shared_dir_path, exist_ok=True)

        return shared_dir_path
