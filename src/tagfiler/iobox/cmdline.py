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
import time

import tagfiler.iobox.dao as dao
from tagfiler.iobox.models import Outbox, Root
import tagfiler.iobox.outbox as outbox

logger = logging.getLogger(__name__)

# Exit return codes
__EXIT_SUCCESS = 0
__EXIT_FAILURE = 1

def create_temp_outbox_dao(**kwargs):
    """Creates an OutboxDAO using a temporary file.
    
    This is useful for command-line instances of the Outbox that do not have a
    saved configuration. It accepts 'kwargs' that are directly passed to the
    OutboxDAO initializer.
    
    Returns a tuple of (outbox_path, outbox_dao) where 'outbox_path' is the
    pathname to the temporary file that contains the Outbox database, and
    'outbox_dao' is an instance of the OutboxDAO.
    """
    (outbox_file, outbox_path) = tempfile.mkstemp()
    logger.debug("create_temp_outbox_dao: %s" % outbox_path)
    outbox_dao = dao.OutboxDAO(outbox_path, **kwargs)
    return (outbox_path, outbox_dao)

def remove_temp_outbox_dao(outbox_path, outbox_dao):
    """Removes an OutboxDAO backed by a temporary file.
    
    Arguments:
        'outbox_path': required pathname to the temporary file.
        'outbox_dao': required instance of OutboxDAO.
    """
    logger.debug("remove_temp_outbox_dao: %s" % outbox_path)
    outbox_dao.close()
    os.unlink(outbox_path)

def main():
    """
    The main routine.
    """
    
    # TODO: Use a verbosity flag to set the level
    logging.basicConfig(level=logging.DEBUG)
    
    # Create the DAO
    p = {'outbox_name':'temp_outbox', 'tagfiler_url':'https://host:port/tagfiler', 'tagfiler_username':'username', 'tagfiler_password':'password'}
    (outbox_path, outbox_dao) = create_temp_outbox_dao(**p)
    
    # Get the Outbox model object
    outbox_model = outbox_dao.get_outbox_by_name('temp_outbox')
    root = Root()
    root.set_filename("/tmp")
    outbox_dao.add_root_to_outbox(outbox_model, root)
    
    # TODO: should move these to a shutdown() function
    remove_temp_outbox_dao(outbox_path, outbox_dao)
    return 0

    outbox_worker = outbox.Outbox(None)
    outbox_worker.start()
    # this doesn't exactly work and it is NOT how I want this to work, just a
    # little Q&D for now
    outbox_worker.join()
    
    print "main() done"
    
    return __EXIT_SUCCESS

