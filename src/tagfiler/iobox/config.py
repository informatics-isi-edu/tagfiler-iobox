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
Command-line configuration utility for the Tagfiler Outbox.
"""

import os
import logging
import argparse

import dao, models, version


logger = logging.getLogger(__name__)

# Exit return codes
__EXIT_SUCCESS = 0
__EXIT_FAILURE = 1

# Used by ArgumentParser
__PROG = "tagfiler-outbox-config"
__DESC = "Tagfiler Outbox configuration utility"
__VER  = version.VERSION_STRING

# Verbosity to Loglevel dictionary
__LOGLEVEL = {0: logging.ERROR,
              1: logging.WARNING,
              2: logging.INFO,
              3: logging.DEBUG}
__LOGLEVEL_MAX = 3
__LOGLEVEL_DEFAULT = 0

def load_or_create_outbox(path):
    """Loads or creates an Outbox.
    
    Returns a tuple consisting of (outbox_dao, outbox_model).
    
    The 'path' is a pathname to the outbox database (i.e., .../outbox.conf).
    """
    dirname = os.path.dirname(path)
    if not os.path.exists(dirname):
        os.makedirs(dirname, 0700)
    outbox_dao = dao.OutboxDAO(path)
    outbox_model = outbox_dao.find_outbox_by_name('default')
    if outbox_model is None:
        outbox_model = models.Outbox(name='default')
        outbox_dao.add_outbox(outbox_model)
    return (outbox_dao, outbox_model)


def main(args=None):
    """
    The main routine.
    
    Optionally accepts 'args' but this is more of a convenience for unit 
    testing this module. It passes 'args' directly to the ArgumentParser's
    parse_args(...) method. If none it will take args from the system directly.
    """
    
    # Use home directory as default location for outbox.conf
    default_config_path = os.path.join(os.path.expanduser('~'), '.tagfiler', 'outbox.conf')
    
    parser = argparse.ArgumentParser(prog=__PROG, description=__DESC)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-v', '--verbose', action='count', default=__LOGLEVEL_DEFAULT)
    group.add_argument('-q', '--quiet', action='store_true')
    parser.add_argument('--version', action='version', version=__VER)
    parser.add_argument('-l' '--list', action='store_true', help='list the configuration')
    parser.add_argument('-f', '--filename', str=argparse.FileType, 
                        help='Outbox configuration filename (default:%s)' % default_config_path)
    args = parser.parse_args(args)
    
    # Turn verbosity into a loglevel setting for the global logger
    if args.quiet:
        logging.getLogger().addHandler(logging.NullHandler())
        # Should probably suppress stderr and stdout
    else:
        verbosity = args.verbose if args.verbose < __LOGLEVEL_MAX else __LOGLEVEL_MAX
        logging.basicConfig(level=__LOGLEVEL[verbosity])
        logger.debug(args)
    
    # Get the Outbox model object
    (outbox_dao, outbox_model) = load_or_create_outbox(args.filename)
    state_dao = outbox_dao.get_state_dao(outbox_model)
    
    # Temp Outbox arguments
    outbox_args = {'outbox_name': args.filename,
                   'tagfiler_url': args.URL,
                   'tagfiler_username': args.username,
                   'tagfiler_password': args.password}
    
    # Add include/exclusion patterns
    if args.exclude:
        expat = models.ExclusionPattern(pattern=args.exclude)
        outbox_model.add_exclusion_pattern(expat)
        
    if args.include:
        inpat = models.InclusionPattern(pattern=args.include)
        outbox_model.add_inclusion_pattern(inpat)
    
    # Add the roots from the command-line
    for rootdir in args.rootdir:
        root = models.Root()
        root.set_filepath(rootdir)
        outbox_dao.add_root_to_outbox(outbox_model, root)
    
    state_dao.close()
    outbox_dao.close()
    return __EXIT_SUCCESS
