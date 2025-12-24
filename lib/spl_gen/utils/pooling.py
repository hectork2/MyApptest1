import json
import socket

import splunk
import splunk.rest
from splunk.clilib import cli_common


def get_server_info(key, session_key):
    '''Return the requested key from the merged values in server.conf.
    If key is unspecified, returns the entire dictionary.
    Uses REST; caller is responsible for exception handling.'''
    unused_response, content = splunk.rest.simpleRequest(
        '/server/info/server-info', sessionKey=session_key, getargs={'output_mode': 'json'}
    )

    if key:
        return json.loads(content)['entry'][0]['content'][key]
    return json.loads(content)['entry'][0]['content']


def get_host_info():
    '''Return tuple of serverName and hostname.'''
    server_name = cli_common.getConfKeyValue('server', 'general', 'serverName')
    hostname = socket.gethostname()
    return server_name, hostname


def is_simple_primary(primary_name):
    '''Perform a simple match of primary_name against the current server name.'''
    server_name, hostname = get_host_info()
    if primary_name:
        return primary_name in (server_name, hostname)


# Search Head Clustering utility functions.
def is_cluster_member(session_key):
    '''Determines whether or not this server is a member of a search head cluster
    @param  session_key: A valid splunk session key

    @return bool:      Returns True if a mode was successfully retrieved
                       and was anything other than 'disabled'.
                       Returns False if a mode was successfully retrieved and was 'disabled';
                       or if splunk.LicenseRestriction was caught.
    @raise Exception:  Raises Exception if mode could not be determined
    '''
    uri = 'shcluster/config/config'
    getargs = {'output_mode': 'json', 'count': 0}

    try:
        r, c = splunk.rest.simpleRequest(uri, sessionKey=session_key, getargs=getargs)

        if r.status == 200:
            c = json.loads(c)['entry'][0]['content']

            if c['mode'] == 'disabled':
                return False

            return True

        raise Exception('Could not determine status of search head clustering')
    # SOLNESS-12151 - handle dev licensing
    except splunk.LicenseRestriction:
        return False


def get_captain(config, return_mgmt_uri=False):
    field_name = 'label'
    if return_mgmt_uri:
        field_name = 'mgmt_uri'

    return config.get('captain', {}).get(field_name)


def get_peer_names(config, active=None, exclude_primary=False, return_mgmt_uri=False):
    peers = []

    field_name = 'label'
    if return_mgmt_uri:
        field_name = 'mgmt_uri'

    captain = get_captain(config, return_mgmt_uri=return_mgmt_uri)

    if active:
        peers = [
            v[field_name] for v in config.get('peers', {}).values() if v.get('status') == 'Up' and v.get(field_name)
        ]
    else:
        peers = [v[field_name] for v in config.get('peers', {}).values() if v.get(field_name)]

    if captain and exclude_primary:
        try:
            peers.remove(captain)
        except Exception:
            pass

    return peers


def is_cluster_primary(session_key, use_alpha, exclude_primary):
    '''Determine whether modular input should execute on a cluster member.
    Order of evaluation:
    1. If primary_name is not None, execute if it matches the serverName. This
       is used to manually specify a specific host for execution.
    2. If primary_name is None, check the value of use_alpha.
        a. If use_alpha is True, execute if the serverName is alphabetically
           first among the active peers in the cluster, excluding the cluster
           primary if specified. This can be used to offload work from the
           cluster captain.
        b. If use_alpha is False, execute if the current peer is the captain.
           This is the default (specified by use_alpha=None in should_execute(),
           below).
    '''
    server_name, hostname = get_host_info()
    config = {}

    uri = 'shcluster/status'
    getargs = {'output_mode': 'json', 'count': 0}

    try:
        r, c = splunk.rest.simpleRequest(uri, sessionKey=session_key, getargs=getargs)

        if r.status == 200:
            config = json.loads(c)['entry'][0]['content']
        # Search Head Clustering is not enabled on this node.
        # REST endpoint is not available
        elif r.status == 503:
            return False
        else:
            raise Exception('Could not determine status of search head clustering')
    # SOLNESS-12151 - handle dev licensing
    except splunk.LicenseRestriction:
        return False

    if use_alpha is True:
        # See if this server is alphabetically first among all the ACTIVE peers. If
        # so, execute on this system.
        active_peers = sorted(get_peer_names(config, active=True, exclude_primary=exclude_primary))
        return active_peers and (server_name == active_peers[0] or hostname == active_peers[0])

    captain = get_captain(config)
    return captain in (server_name, hostname)


# Cloud utility functions
def is_cloud_instance(session_key):
    try:
        instance_type = get_server_info('instance_type', session_key)
        return instance_type == 'cloud'
    except KeyError:
        return False


# Modular input execution utility functions.
def should_execute(primary_host=None, session_key=None, use_alpha=None, exclude_primary=None):
    '''Determine if a modular input should execute on this host. Execute in
    the following conditions:

        1. We are in SHC (search head clustering) configuration.
            a. The value of primary_host matches our hostname or server name.
            b. The value of use_alpha is True, and the value of the current
               server's hostname or server name is alphabetically first in the
               list of active peers, excluding the primary.
            c. The value of use_alpha evaluates to False, and the value of the
               current server's hostname or server name is the same as the cluster
               primary.
        2. We are not in SHC configuration. The primary_host setting is
           ignored and execution proceeds.

    Returns a tuple (boolean_status, string_msg) indicating whether this host
    should execute the modular input or not, and the reason.
    '''

    # If/else logic is slightly complex to permit return of specific error
    # messages to the caller.  Assume SHP not enabled until proven otherwise.

    output_fmt = (
        'will_execute="{0}" config="{1}" msg="{2}" primary_host="{3}" use_alpha="{4}" exclude_primary="{5}"'.format
    )
    rv = (
        True,
        output_fmt(
            True,
            "standalone",
            "Standalone server; modular input execution proceeding.",
            primary_host,
            use_alpha,
            exclude_primary,
        ),
    )

    if is_cluster_member(session_key):
        if primary_host:
            # primary host was defined in the inputs.conf stanza.
            if is_simple_primary(primary_host):
                rv = (True, output_fmt(True, 'SHC', "primary host matched.", primary_host, use_alpha, exclude_primary))
            else:
                rv = (
                    False,
                    output_fmt(False, 'SHC', "primary host unmatched.", primary_host, use_alpha, exclude_primary),
                )
        else:
            # Try to select primary host alphabetically.
            if is_cluster_primary(session_key, use_alpha, exclude_primary):
                rv = (
                    True,
                    output_fmt(
                        True,
                        'SHC',
                        "Selected based on SHC primary selection algorithm.",
                        primary_host,
                        use_alpha,
                        exclude_primary,
                    ),
                )
            else:
                rv = (
                    False,
                    output_fmt(
                        False,
                        'SHC',
                        "Deselected based on SHC primary selection algorithm.",
                        primary_host,
                        use_alpha,
                        exclude_primary,
                    ),
                )

    return rv
