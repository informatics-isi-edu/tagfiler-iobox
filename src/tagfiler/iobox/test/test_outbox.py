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

import base
from tagfiler.iobox import outbox

import unittest
import logging
import time


logger = logging.getLogger(__name__)


def all_tests():
    """Returns a TestSuite that includes all test cases in this module."""
    suite = unittest.TestSuite()
    suite.addTest(OutboxManagerTest())
    return suite


class OutboxManagerTest(base.OutboxBaseTestCase):
    """Test of the Outbox Manager."""
    
    def runTest(self):
        outbox_worker = outbox.Outbox(self.outbox_model)
        outbox_worker.start()
        outbox_worker.join() # TODO: Fix this Q&D synchronization!
        outbox_worker.terminate()
        time.sleep(1)
        self.assertTrue(outbox_worker.is_terminated(), 'Outbox has not terminated')


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()