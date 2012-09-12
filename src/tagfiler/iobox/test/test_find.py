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
Unit tests for find module.
"""

import unittest
import logging
import tagfiler.iobox.worker as worker
import tagfiler.iobox.find as find

logger = logging.getLogger(__name__)

class Test(unittest.TestCase):


    def setUp(self):
        """TODO: We should create a tmp dir with a known structure so that in
        the tests we can make sure to find just the number of files and dirs
        that we expect to find."""
        pass


    def tearDown(self):
        """TODO: Then we should clean up the temp dir."""
        pass


    def testFind(self):
        """Simple test for the Find worker."""
        
        find_q = worker.WorkQueue()
        tag_q = worker.WorkQueue()
    
        # In this test we will inspect the /tmp directory.
        # TODO: we ought to create a new directory and test it instead.
        find_q.put("/tmp")
        find_q.put(worker.Worker.DONE)
        
        find_worker = find.Find(find_q, tag_q)
        find_worker.start()
        find_q.join()


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()