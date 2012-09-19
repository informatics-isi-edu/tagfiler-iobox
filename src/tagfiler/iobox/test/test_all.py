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

import test_dao, test_http, test_worker
import test_find, test_tag, test_register, test_outbox

import unittest
import logging


logger = logging.getLogger(__name__)


def all_tests():
    """Returns a test suite containing all test suites for all modules."""
    suite = unittest.TestSuite()
    suite.addTest(test_worker.all_tests())
    suite.addTest(test_http.all_tests())
    suite.addTest(test_dao.all_tests())
    suite.addTest(test_find.all_tests())
    suite.addTest(test_tag.all_tests())
    suite.addTest(test_register.all_tests())
    suite.addTest(test_outbox.all_tests())
    # New test suites should be added here...
    return suite


if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)
    runner = unittest.TextTestRunner()
    test_suite = all_tests()
    runner.run(test_suite)
