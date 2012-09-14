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

import os
import unittest
import logging
import tempfile
import shutil
import time

import tagfiler.iobox.worker as worker
import tagfiler.iobox.find as find

logger = logging.getLogger(__name__)

class Test(unittest.TestCase):

    __NUMROOTS = 2
    __NUMDIRS = 5
    __NUMFILES = 10
    
    def setUp(self):
        """Create a directory tree."""
        self.rootdirs = []
        for r in range(Test.__NUMROOTS):
            rootdir = tempfile.mkdtemp()
            logger.debug("setUp: rootdir: %s" % rootdir)
            self.rootdirs.append(rootdir)
            for i in range(Test.__NUMDIRS):
                currdir = tempfile.mkdtemp(dir=rootdir)
                for j in range(Test.__NUMFILES):
                    tempfile.mkstemp(dir=currdir)
        
    def tearDown(self):
        """Removes the test directory tree."""
        for rootdir in self.rootdirs:
            logger.debug("tearDown: rootdir: %s" % rootdir)
            shutil.rmtree(rootdir, ignore_errors=True)

    def testFind(self):
        """Simple test for the Find worker."""
        
        find_q = worker.WorkQueue()
        tag_q = worker.WorkQueue()
    
        for rootdir in self.rootdirs:
            find_q.put(rootdir)
        
        find_worker = find.Find(find_q, tag_q)
        find_worker.start()
        find_q.join()
        find_worker.terminate()
        
        assert tag_q.qsize() == (Test.__NUMROOTS + 
                                 Test.__NUMROOTS * Test.__NUMDIRS +
                                 Test.__NUMROOTS * Test.__NUMDIRS * Test.__NUMFILES)
        
        time.sleep(1)
        assert not find_worker.is_alive()


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()