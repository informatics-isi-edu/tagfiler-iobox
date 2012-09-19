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
Unit tests for tag module.
"""

import unittest
import logging

import base


def all_tests():
    """Returns a TestSuite that includes all test cases in this module."""
    suite = unittest.TestSuite()
    suite.addTest(TagTest())
    return suite


class TagTest(base.OutboxBaseTestCase):
    
    def runTest(self):
        pass

if __name__ == "__main__":
    logging.basicConfig(logging.DEBUG)
    unittest.main()
