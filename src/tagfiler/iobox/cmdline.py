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

import os
import logging
import tempfile
import argparse

import dao, models, outbox, version
import config # TODO: refactor this out, no crossrefs

logger = logging.getLogger(__name__)

# Exit return codes
__EXIT_SUCCESS = 0
__EXIT_FAILURE = 1

# Used by ArgumentParser
__PROG = "tagfiler-outbox"
__DESC = "Tagfiler Outbox command-line interface"
__VER  = version.VERSION_STRING

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
    try:
        os.unlink(outbox_path)
    except:
        logger.warn("Could not remove temporary outbox dao %s" % outbox_path)

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
    parser.add_argument('-p', '--print', dest='dump', action='store_true', 
                        help='print the configuration values')
    parser.add_argument('-n', '--name', type=str, default='default',
                        help='name of the outbox to configure')
    # Use home directory as default location for outbox.conf
    default_config_path = os.path.join(os.path.expanduser('~'), 
                                       '.tagfiler', 'outbox.conf')
    parser.add_argument('-f', '--filename', type=str, 
                        default=default_config_path,
                        help=('configuration filename (default: %s)' % 
                              default_config_path))
    
    # Verbose | Quite option group
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-v', '--verbose', action='count', 
                       default=__LOGLEVEL_DEFAULT, help='verbose output')
    group.add_argument('-q', '--quiet', action='store_true', 
                       help='suppress output')
    
    # Inclusion/Exclusion option group
    group = parser.add_argument_group(title='File filters')
    group.add_argument('--include', type=str, nargs='+',
                       help='regular expression for includes pattern')
    group.add_argument('--exclude', type=str, nargs='+',
                       help='regular expression for excludes pattern')
    
    # Tagfiler option group
    group = parser.add_argument_group(title='Tagfiler options')
    group.add_argument('--url', dest='url', metavar='URL', 
                       type=str, help='URL used to connect to Tagfiler')
    group.add_argument('--username', dest='username', metavar='USERNAME', 
                       type=str, help='username used when connecting to Tagfiler')
    group.add_argument('--password', dest='password', metavar='PASSWORD', 
                       type=str, help='password used when connecting to Tagfiler')
    
    # Roots option group
    group = parser.add_argument_group(title='Root directory options')
    group.add_argument('--rootdir', metavar='DIRECTORY', 
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
        logger.debug(args)
    
    # Get the Outbox model object
    (outbox_dao, outbox_model) = config.load_or_create_outbox(
                                                    args.name, 
                                                    args.filename)
    state_dao = outbox_dao.get_state_dao(outbox_model)
    
    # Add include/exclusion patterns
    if args.exclude:
        expat = models.ExclusionPattern(pattern=args.exclude)
        outbox_model.add_exclusion_pattern(expat)
        
    if args.include:
        inpat = models.InclusionPattern(pattern=args.include)
        outbox_model.add_inclusion_pattern(inpat)
    
    # Add the roots from the command-line
    if args.rootdir:
        for rootdir in args.rootdir:
            root = models.Root()
            root.set_filepath(rootdir)
            outbox_model.add_root(root) # temporarily add root
        
    # Set tagfiler settings
    if args.url or args.username or args.password:
        tagfiler = outbox_model.get_tagfiler()
        if args.url:
            tagfiler.set_url(args.url)
        if args.username:
            tagfiler.set_username(args.username)
        if args.password:
            tagfiler.set_password(args.password)
    
    # Dump the outbox to STDOUT
    if args.dump:
        config.dump_outbox(outbox_model)
    
    outbox_manager = outbox.Outbox(outbox_model, state_dao)
    outbox_manager.start()
    outbox_manager.join()
    outbox_manager.terminate()
    
    outbox_dao.close()
    return __EXIT_SUCCESS
