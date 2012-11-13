# 
# Copyright 2010 University of Southern California
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Command-line interface for the Tagfiler Outbox.
"""

import outbox
import version
from models import RERule, Outbox
from tagfiler.util.http import TagfilerClient, AddressError, NetworkError, NotFoundError

import os
import logging
import argparse
import json
import time
import socket
import re


logger = logging.getLogger(__name__)

# Exit return codes
__EXIT_SUCCESS = 0
__EXIT_FAILURE = 1

# Used by ArgumentParser
__PROG = "tagfiler-outbox"
__DESC = "Tagfiler Outbox command-line interface"
__VER  = version.VERSION_STRING
__DEFAULT_OUTBOX_NAME = "outbox"
__BULK_OPS_MAX = 1000

# Verbosity to Loglevel dictionary
__LOGLEVEL = {0: logging.ERROR,
              1: logging.WARNING,
              2: logging.INFO,
              3: logging.DEBUG}
__LOGLEVEL_MAX = 3
__LOGLEVEL_DEFAULT = 0


def create_default_name_path_rule(endpoint_name):
    """Creates the path rule for the required 'name' tag."""
    path_rule = RERule()
    path_rule.pattern = '^(?P<path>.*)'
    path_rule.extract = 'template'
    path_rule.templates.append('file://%s\g<path>' % endpoint_name)
    path_rule.tags.append('name')
    return path_rule


def main(args=None):
    """
    The main routine.
    
    Optionally accepts 'args' but this is more of a convenience for unit 
    testing this module. It passes 'args' directly to the ArgumentParser's
    parse_args(...) method.
    """
    parser = argparse.ArgumentParser(prog=__PROG, description=__DESC)

    # General options
    parser.add_argument('--version', action='version', version=__VER)
    parser.add_argument('-n', '--name', type=str, default='default',
                        help='name of the outbox configuration')
    # Use home directory as default location for outbox.conf
    default_config_path = os.path.join(os.path.expanduser('~'), 
                                       '.tagfiler', 'outbox.conf')
    parser.add_argument('-f', '--filename', type=str, 
                        default=default_config_path,
                        help=('configuration filename (default: %s)' % 
                              default_config_path))
    # Use home directory as default location for state.db
    default_state_db = os.path.join(os.path.expanduser('~'), 
                                    '.tagfiler', 'state.db')
    parser.add_argument('-s', '--state_db', type=str, 
                        default=default_state_db,
                        help=('local state database (default: %s)' % 
                              default_state_db))
    # Use hostname as default endpoint_name
    default_endpoint_name = socket.gethostname()
    parser.add_argument('-e', '--endpoint_name', type=str,
                        default=default_endpoint_name,
                        help=('endpoint name (default: %s)' % 
                              default_endpoint_name))
    
    # Verbose | Quite option group
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-v', '--verbose', action='count', 
                       default=__LOGLEVEL_DEFAULT, 
                       help='verbose output (repeat to increase verbosity)')
    group.add_argument('-q', '--quiet', action='store_true', 
                       help='suppress output')
    
    # Inclusion/Exclusion option group
    group = parser.add_argument_group(title='File filters')
    group.add_argument('--include', type=str, nargs='+',
                       help='inclusion pattern (regular expression)')
    group.add_argument('--exclude', type=str, nargs='+',
                       help='exclusion pattern (regular expression)')
    
    # Tagfiler option group
    group = parser.add_argument_group(title='Tagfiler options')
    group.add_argument('--url', dest='url', metavar='URL', 
                       type=str, help='URL used to connect to Tagfiler')
    group.add_argument('--username', dest='username', metavar='USERNAME', 
                       type=str, help='username used when connecting to Tagfiler')
    group.add_argument('--password', dest='password', metavar='PASSWORD', 
                       type=str, help='password used when connecting to Tagfiler')
    group.add_argument('--bulk_ops_max', type=int, 
                        help='maximum bulk operations per Tagfiler call' + \
                        ' (default: %d)' % __BULK_OPS_MAX)
    
    # Roots option group
    group = parser.add_argument_group(title='Root directory options')
    group.add_argument('--root', metavar='DIRECTORY', 
                       type=str, nargs='+',
                       help='add root directories to the base configuration')
    args = parser.parse_args(args)
    
    # Turn verbosity into a loglevel setting for the global logger
    if args.quiet:
        logging.getLogger().addHandler(logging.NullHandler())
        # Should probably suppress stderr and stdout
    else:
        verbosity = args.verbose if args.verbose < __LOGLEVEL_MAX else __LOGLEVEL_MAX
        logging.basicConfig(level=__LOGLEVEL[verbosity])
        logger.debug("args: %s" % args)
    
    # Load configuration file, or create configuration based on arguments
    cfg = {}
    if os.path.exists(args.filename):
        f = open(args.filename, 'r')
        try:
            cfg = json.load(f)
            logger.debug("config: %s" % cfg)
        except ValueError as e:
            logger.error('Malformed configuration file: %s', e)
            return __EXIT_FAILURE
        else:
            f.close()
    
    # Create outbox model, and populate from settings
    outbox_model = Outbox()
    outbox_model.name = args.name or cfg.get('name', __DEFAULT_OUTBOX_NAME)
    outbox_model.state_db = args.state_db or \
                            cfg.get('state_db', default_state_db)

    # Tagfiler settings
    outbox_model.url = args.url or cfg.get('url')
    if not outbox_model.url:
        parser.error('Tagfiler URL must be given.')
    
    outbox_model.username = args.username or cfg.get('username')
    if not outbox_model.username:
        parser.error('Tagfiler username must be given.')
    
    outbox_model.password = args.password or cfg.get('password')
    if not outbox_model.password:
        parser.error('Tagfiler password must be given.')
        
    outbox_model.bulk_ops_max = args.bulk_ops_max or \
                                cfg.get('bulk_ops_max', __BULK_OPS_MAX)
    outbox_model.endpoint_name = args.endpoint_name or \
                                cfg.get('endpoint_name', default_endpoint_name)

    # Roots
    roots = args.root or cfg.get('roots')
    for root in roots:
        outbox_model.roots.append(root)
    if not len(roots):
        parser.error('Must specify at least one root directory.')
    
    # Add include/exclusion patterns
    excludes = args.exclude or cfg.get('excludes')
    for exclude in excludes:
        outbox_model.excludes.append(re.compile(exclude))
    
    includes = args.include or cfg.get('includes')
    for include in includes:
        outbox_model.includes.append(re.compile(include))
    
    # Add the default 'name' tag path rule
    name_rule = create_default_name_path_rule(outbox_model.endpoint_name)
    outbox_model.path_rules.append(name_rule)
    
    # Add optional path rules
    rules = cfg.get('rules', [])
    for rule in rules:
        outbox_model.path_rules.append(RERule(**rule))

    # Establish Tagfiler client connection
    client = TagfilerClient(outbox_model.url, outbox_model.username, 
                            outbox_model.password)
    try:
        client.connect()
        client.login()
    except AddressError as err:
        logger.error(err)
        return __EXIT_FAILURE
    except NetworkError as err:
        logger.error(err)
        return __EXIT_FAILURE
    except NotFoundError as err:
        logger.error(err)
        return __EXIT_FAILURE
    
    # Now, create the outbox manager and let it run to completion
    outbox_manager = outbox.Outbox(outbox_model, client)
    outbox_manager.start()
    outbox_manager.done()
    outbox_manager.wait_done()
    logger.debug("done")
    outbox_manager.terminate()
    while not outbox_manager.is_alive():
        time.sleep(1) # TODO: Maybe should implement another callback in outbox...
        
    client.close()
    return __EXIT_SUCCESS
