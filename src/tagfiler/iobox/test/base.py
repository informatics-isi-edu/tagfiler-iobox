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
Shared utilities for Outbox TestCases.
"""

from tagfiler.iobox.cmdline import create_temp_outbox_dao, remove_temp_outbox_dao
from tagfiler.iobox import models

import unittest
import logging
import tempfile
import shutil


logger = logging.getLogger(__name__)

test_endpoint_name = "smithd#tagfiler_ep"

def create_test_outbox():
    outbox = models.Outbox()
    outbox.set_name('test_outbox')
    tagfiler = models.Tagfiler()
    tagfiler.set_url('https://jacoby.isi.edu/tagfiler')
    tagfiler.set_username('smithd')
    tagfiler.set_password('smithd')
    outbox.set_tagfiler(tagfiler)
    return outbox

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

def create_date_and_study_path_rule():
    path_rule = models.PathRule()
    path_rule.set_pattern('^/.*/studies/([^/]+)/([^/]+)/')
    path_rule.set_extract('positional')
    date_tag = models.RERuleTag()
    date_tag.set_tag_name('date')
    session_tag = models.RERuleTag()
    session_tag.set_tag_name('session')
    path_rule.set_tags([date_tag, session_tag])
        
    return path_rule

def create_name_path_rule():
    path_rule = models.PathRule()
    path_rule.set_pattern('^(?P<path>.*)')
    path_rule.set_extract('template')
    t1 = models.RERuleTemplate()
    t1.set_template('file://%s\g<path>' % test_endpoint_name)
    path_rule.add_template(t1)
    tg1 = models.RERuleTag()
    tg1.set_tag_name('name')
    path_rule.add_tag(tg1)
    
    return path_rule

class OutboxBaseTestCase(unittest.TestCase):
    """Base class for Outbox TestCases.
    
    This TestCase subclass implements setUp and tearDown methods that should 
    be applicable to a wide range of Outbox-related test cases.
    """

    def get_numroots(self):
        """Returns the number of roots to be used in the test.
        
        Subclasses of this class can override this method to return any 
        positive integer number of roots.
        """
        return 1
    
    def get_numdirs(self):
        """Returns the number of directories to be populated in each root 
        directory.
        
        Subclasses of this class can override this method to return any 
        positive integer number of directories per root.
        """
        return 1
    
    def get_numfiles(self):
        """Returns the number of files per directory to be used in the test.
        
        Subclasses of this class can override this method to return any 
        positive integer number of roots.
        """
        return 10
    
    def setUp(self):
        """Subclasses should call this setUp method before class specific
        setup."""
        # Create the test directory tree
        self.rootdirs = create_temp_dirtree(self.get_numroots(), 
                                            self.get_numdirs(), 
                                            self.get_numfiles())
        
        # Create the temporary OutboxDAO and Outbox
        (self.outbox_path, self.outbox_dao) = create_temp_outbox_dao()
        self.outbox_model = self.outbox_dao.add_outbox(create_test_outbox())
        self.state_dao = self.outbox_dao.get_state_dao(self.outbox_model)
        
        # Add the roots to the Outbox model object
        for rootdir in self.rootdirs:
            root = models.Root()
            root.set_filepath(rootdir)
            self.outbox_dao.add_root_to_outbox(self.outbox_model, root)
    
    def tearDown(self):
        """Subclasses should do class specific teardown before calling this 
        tearDown method."""
        # Remove the test directory tree
        remove_temp_dirtree(self.rootdirs)
        
        # Remove the temporary OutboxDAO
        remove_temp_outbox_dao(self.outbox_path, self.outbox_dao)
