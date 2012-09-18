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
Unit tests for the outbox module.
"""

from tagfiler.iobox.test.test_find import create_temp_dirtree, remove_temp_dirtree
from tagfiler.iobox.cmdline import create_temp_outbox_dao, remove_temp_outbox_dao
from tagfiler.iobox import models, outbox

import unittest
import logging
import time

logger = logging.getLogger(__name__)


class OutboxTest(unittest.TestCase):

    __OUTBOX_NAME   =  'temp_outbox'
    __NUMROOTS      =  1
    __NUMDIRS       =  1
    __NUMFILES      =  10
    
    def setUp(self):
        # Create the test directory tree
        self.rootdirs = create_temp_dirtree(OutboxTest.__NUMROOTS, 
                                            OutboxTest.__NUMDIRS, 
                                            OutboxTest.__NUMFILES)
        
        # Create the temporary OutboxDAO and Outbox
        p = {'outbox_name':OutboxTest.__OUTBOX_NAME, 'tagfiler_url':'https://host:port/tagfiler', 'tagfiler_username':'username', 'tagfiler_password':'password'}
        (self.outbox_path, self.outbox_dao) = create_temp_outbox_dao()
        self.outbox_model = models.Outbox(**p)
        self.outbox_model.set_name(OutboxTest.__OUTBOX_NAME)
        self.outbox_model = self.outbox_dao.add_outbox(self.outbox_model)
        self.state_dao = self.outbox_dao.get_state_dao(self.outbox_model)
        
        # Add the roots to the Outbox model object
        for rootdir in self.rootdirs:
            root = models.Root()
            root.set_filepath(rootdir)
            self.outbox_dao.add_root_to_outbox(self.outbox_model, root)
    
    def tearDown(self):
        # Remove the test directory tree
        remove_temp_dirtree(self.rootdirs)
        
        # Remove the temporary OutboxDAO
        remove_temp_outbox_dao(self.outbox_path, self.outbox_dao)

    def testBaseline(self):
        """Simple test for the Outbox."""
        outbox_worker = outbox.Outbox(self.outbox_model, self.state_dao)
        outbox_worker.start()
        outbox_worker.join() # TODO: Fix this Q&D synchronization!
        outbox_worker.terminate()
        time.sleep(1)
        self.assertTrue(outbox_worker.is_terminated(), 'Outbox has not terminated')
        logger.debug("testOutbox: done")


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()