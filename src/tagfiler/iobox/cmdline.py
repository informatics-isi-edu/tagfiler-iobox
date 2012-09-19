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
The command-line interface to the tagfiler-outbox service.
"""

import os
import logging
import tempfile
import argparse
import sys

import tagfiler.iobox.dao as dao
import tagfiler.iobox.models as models
import tagfiler.iobox.outbox as outbox

logger = logging.getLogger(__name__)

# Exit return codes
__EXIT_SUCCESS = 0
__EXIT_FAILURE = 1

# Used by ArgumentParser
__PROG = "tagfiler-outbox"
__DESC = "Tagfiler Outbox"
__VER  = "%(prog)s 0.1 beta" # (schuler): Just making that up!

# Verbosity to Loglevel dictionary
__LOGLEVEL = {0: logging.ERROR,
              1: logging.WARNING,
              2: logging.INFO,
              3: logging.DEBUG}
__LOGLEVEL_MAX = 3
__LOGLEVEL_DEFAULT = 0

def create_temp_outbox_dao():
    """Creates an OutboxDAO using a temporary file.
    
    This is intended for command-line instances of the Outbox that do not have
    a saved configuration.
    
    Returns a tuple of (outbox_path, outbox_dao) where 'outbox_path' is the
    pathname to the temporary file that contains the Outbox database, and
    'outbox_dao' is an instance of the OutboxDAO.
    """
    (outbox_file, outbox_path) = tempfile.mkstemp()
    logger.debug("create_temp_outbox_dao: %s" % outbox_path)
    outbox_dao = dao.OutboxDAO(outbox_path)
    return (outbox_path, outbox_dao)

def remove_temp_outbox_dao(outbox_path, outbox_dao):
    """Closes and removes an OutboxDAO backed by a temporary file.
    
    Arguments:
        'outbox_path': required pathname to the temporary file.
        'outbox_dao': required instance of OutboxDAO.
    """
    logger.debug("remove_temp_outbox_dao: %s" % outbox_path)
    outbox_dao.close()
    os.unlink(outbox_path)

def main(args=None):
    """
    The main routine.
    
    Optionally accepts 'args' but this is more of a convenience for unit 
    testing this module. It passes 'args' directly to the ArgumentParser's
    parse_args(...) method.
    """
    parser = argparse.ArgumentParser(prog=__PROG, description=__DESC)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-v', '--verbose', action='count', default=__LOGLEVEL_DEFAULT)
    group.add_argument('-q', '--quiet', action='store_true')
    parser.add_argument('--version', action='version', version=__VER)
    parser.add_argument('-i', '--inclusion-pattern', help='inclusion pattern')
    parser.add_argument('-x', '--exclusion-pattern', help='exclusion pattern')
    parser.add_argument('URL', type=str, help='URL of the Tagfiler service (example: https://host/tagfiler)')
    parser.add_argument('username', type=str, help='Username for Tagfiler authentication')
    parser.add_argument('password', type=str, help='Password for Tagfiler authentication')
    parser.add_argument('rootdir', nargs='+', type=str, 
                        help='root directories of the outbox')
    args = parser.parse_args(args)
    
    # Turn verbosity into a loglevel setting for the global logger
    if args.quiet:
        logging.getLogger().addHandler(logging.NullHandler())
        # Should probably suppress stderr and stdout
    else:
        verbosity = args.verbose if args.verbose < __LOGLEVEL_MAX else __LOGLEVEL_MAX
        logging.basicConfig(level=__LOGLEVEL[verbosity])
    
    # Create the DAO
    (outbox_path, outbox_dao) = create_temp_outbox_dao()
    
    # Temp Outbox arguments
    outbox_args = {'outbox_name': 'temp_outbox',
                   'tagfiler_url': args.URL,
                   'tagfiler_username': args.username,
                   'tagfiler_password': args.password}
    
    # Get the Outbox model object
    outbox_model = models.Outbox(**outbox_args)
    outbox_model = outbox_dao.add_outbox(outbox_model)
    state_dao = outbox_dao.get_state_dao(outbox_model)
    
    # Add the roots from the command-line
    for rootdir in args.rootdir:
        root = models.Root()
        root.set_filepath(rootdir)
        outbox_dao.add_root_to_outbox(outbox_model, root)
    
    outbox_manager = outbox.Outbox(outbox_model, state_dao)
    outbox_manager.start()
    outbox_manager.join()
    outbox_manager.terminate()
    
    # TODO: should move this to a shutdown() function
    remove_temp_outbox_dao(outbox_path, outbox_dao)
    return __EXIT_SUCCESS
