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

def load_or_create_outbox(name, path):
    """Loads or creates an Outbox.
    
    Returns a tuple consisting of (outbox_dao, outbox_model).
    
    The 'name' is the name of the outbox. Each DAO creates or opens a database 
    file. Each database may store 1 or more outbox configurations.
    
    The 'path' is a pathname to the outbox database (i.e., .../outbox.conf).
    """
    dirname = os.path.dirname(path)
    if not os.path.exists(dirname):
        os.makedirs(dirname, 0700)
    outbox_dao = dao.OutboxDAO(path)
    outbox_model = outbox_dao.find_outbox_by_name(name)
    if outbox_model is None:
        outbox_model = models.Outbox(outbox_name=name)
        tagfiler = models.Tagfiler()
        tagfiler.set_url('https://curiosity.isi.edu/tagfiler')
        tagfiler.set_username('demo')
        tagfiler.set_password('demo')
        outbox_model.set_tagfiler(tagfiler)
        outbox_dao.add_outbox(outbox_model)
    return (outbox_dao, outbox_model)


def dump_outbox(outbox_model):
    """Dumps the outbox model to stdout."""
    
    print 'Outbox configuration:'
    print '    Name:        %s' % outbox_model.get_name()
    print
    
    tagfiler = outbox_model.get_tagfiler()
    print '    Tagfiler server:'
    print '    URL:         %s' % tagfiler.get_url()
    print '    Username:    %s' % tagfiler.get_username()
    print '    Password:    %s' % tagfiler.get_password()
    print
    
    roots = outbox_model.get_roots()
    print '    Root directories:'
    for root in roots:
        print '                %s' % root.get_filepath()


def main(args=None):
    """The main routine.
    
    Optionally accepts 'args' but this is more of a convenience for unit 
    testing this module. It passes 'args' directly to the ArgumentParser's
    parse_args(...) method. If none it will take args from the system directly.
    """
    
    # Setup argument parser
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
    
    # Tagfiler option group
    group = parser.add_argument_group(title='Tagfiler options')
    group.add_argument('--set-url', dest='url', metavar='URL', 
                       type=str, help='URL used to connect to Tagfiler')
    group.add_argument('--set-username', dest='username', metavar='USERNAME', 
                       type=str, help='username used when connecting to Tagfiler')
    group.add_argument('--set-password', dest='password', metavar='PASSWORD', 
                       type=str, help='password used when connecting to Tagfiler')
    
    # Roots option group
    group = parser.add_argument_group(title='Root directory options')
    group.add_argument('--add-root', metavar='DIRECTORY', 
                       type=str, nargs='+',
                       help='add a root directory')
    
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
    (outbox_dao, outbox_model) = load_or_create_outbox(args.name, 
                                                       args.filename)
    
    # Add root directories
    if args.add_root:
        for rootdir in args.add_root:
            root = models.Root(filepath=rootdir)
            outbox_dao.add_root_to_outbox(outbox_model, root)
    
    
    # Set tagfiler settings
    if args.url or args.username or args.password:
        tagfiler = outbox_model.get_tagfiler()
        if args.url:
            tagfiler.set_url(args.url)
        if args.username:
            tagfiler.set_username(args.username)
        if args.password:
            tagfiler.set_password(args.password)
        #TODO: need a DAO call to update a tagfiler association with an outbox
        #outbox_dao.update_outbox_tagfiler(outbox_model, tagfiler)
    
    # Dump the outbox to STDOUT
    if args.dump:
        dump_outbox(outbox_model)
    
    outbox_dao.close()
    return __EXIT_SUCCESS
