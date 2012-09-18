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
All unit tests coverage.
"""

import unittest
import logging

from tagfiler.iobox.test.test_dao import TestOutboxDAO, TestOutboxStateDAO
from tagfiler.iobox.test.test_find import FindTest
from tagfiler.iobox.test.test_outbox import OutboxTest
from tagfiler.iobox.test.test_worker import WorkerTest
from tagfiler.iobox.test.test_http import TestTagfilerClient

logger = logging.getLogger(__name__)

def suite():
    suite = unittest.TestSuite()
    suite.addTest(WorkerTest('testSingleStagePipeline'))
#    suite.addTest(TestOutboxDAO())
#    suite.addTest(TestOutboxStateDAO())
#    suite.addTest(FindTest())
#    suite.addTest(OutboxTest())
#    suite.addTest(TestTagfilerClient())
    return suite

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    runner = unittest.TextTestRunner()
    test_suite = suite()
    runner.run(test_suite)
