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

import tagfiler.iobox.worker as worker
import tagfiler.iobox.find as find
import tagfiler.iobox.models as models
import tagfiler.iobox.test.base as base

import unittest
import logging
import tempfile
import shutil
import time


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


class FindTest(base.OutboxBaseTestCase):
    
    def get_numroots(self):
        return 2
    
    def get_numdirs(self):
        return 5
    
    def get_numfiles(self):
        return 10

    def runTest(self):
        """Simple test for the Find worker."""
        
        find_q = worker.WorkQueue()
        tag_q = worker.WorkQueue()
    
        for rootdir in self.rootdirs:
            root = models.Root()
            root.set_filepath(rootdir)
            find_q.put(root)
        
        find_worker = find.Find(find_q, tag_q, self.state_dao)
        find_worker.start()
        find_q.join()
        find_worker.terminate()
        
        self.assertEqual(tag_q.qsize(), 
            (self.get_numroots() + 
             self.get_numroots() * self.get_numdirs() + 
             self.get_numroots() * self.get_numdirs() * self.get_numfiles()), 
            "Failed to find all directories and files in the temp dir")
        
        time.sleep(1) #TODO(schuler): Hate to do it this way
        self.assertFalse(find_worker.is_alive(), "Find has not terminated")


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()