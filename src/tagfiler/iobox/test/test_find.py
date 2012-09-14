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
import tempfile
import shutil
import time

import tagfiler.iobox.worker as worker
import tagfiler.iobox.find as find
import tagfiler.iobox.models as models

logger = logging.getLogger(__name__)


def create_temp_dirtree(numroots, numdirs, numfiles):
    """Creates a temporary directory and returns a list of root 'dirs'."""
    rootdirs = []
    for r in range(numroots):
        rootdir = tempfile.mkdtemp()
        logger.debug("create_temp_dirtree: %s" % rootdir)
        rootdirs.append(rootdir)
        for i in range(numdirs):
            currdir = tempfile.mkdtemp(dir=rootdir)
            for j in range(numfiles):
                tempfile.mkstemp(dir=currdir)
                
    return rootdirs

def remove_temp_dirtree(dirs=[]):
    """Removes directory trees rooted in 'dirs' list."""
    for rootdir in dirs:
        logger.debug("remove_temp_dirtree: %s" % rootdir)
        shutil.rmtree(rootdir, ignore_errors=True)


class Test(unittest.TestCase):

    __NUMROOTS = 2
    __NUMDIRS = 5
    __NUMFILES = 10
    
    def setUp(self):
        """Create a directory tree."""
        self.rootdirs = create_temp_dirtree(Test.__NUMROOTS, 
                                            Test.__NUMDIRS, Test.__NUMFILES)
        
    def tearDown(self):
        """Removes the test directory tree."""
        remove_temp_dirtree(self.rootdirs)

    def testFind(self):
        """Simple test for the Find worker."""
        
        find_q = worker.WorkQueue()
        tag_q = worker.WorkQueue()
    
        for rootdir in self.rootdirs:
            root = models.Root()
            root.set_filename(rootdir)
            find_q.put(root)
        
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