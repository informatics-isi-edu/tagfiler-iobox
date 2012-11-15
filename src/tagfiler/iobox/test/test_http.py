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
Unit tests for the http module.
"""

from tagfiler.iobox.models import File, Tag
import base

import random
import unittest


def all_tests():
    """Returns a TestSuite that includes all test cases in this module."""
    suite = unittest.TestSuite()
    suite.addTest(TagfilerAddAndFindSubjectsTest())
#    suite.addTest(TagfilerAddSubjectsTest())
    return suite


class TagfilerAddAndFindSubjectsTest(unittest.TestCase):

    def setUp(self):
        outbox_model = base.create_test_outbox()
        self.client = base.create_test_client(outbox_model)
    
    def tearDown(self):
        self.client.close()
        
    def runTest(self):
        f = File()
        f.filename = "/home/demo/tagfiler_test%s.jpg" % unicode(random.randint(0,10000))
        f.size = 100
        
        t = Tag()
        t.name = "name"
        name = "file://demo#tagfiler_ep%s" % f.filename
        t.value = name
        f.tags.append(t)
        
        t = Tag()
        t.name = "session"
        t.value = "session9"
        f.tags.append(t)
        
        t = Tag()
        t.name = "sha256sum"
        t.value = "53534mnl5k34n5l34kn5"
        f.tags.append(t)
        
        self.client.add_subject(f)
        result = self.client.find_subject_by_name(name)
        
        assert result is not None
        assert result[0]['name'] == name


class TagfilerAddSubjectsTest(base.OutboxBaseTestCase):

    def setUp(self):
        outbox_model = base.create_test_outbox()
        self.client = base.create_test_client(outbox_model)
    
    def tearDown(self):
        self.client.close()
        
    def runTest(self):
        files = []
        for i in range(1, 10):
            i # is not used
            f = File()
            f.filename = "/home/demo/tagfiler_test%s.jpg" % unicode(random.randint(0,10000))
            f.size = 100
            
            t = Tag()
            t.name = "name"
            t.value = "file://demo#tagfiler_ep%s" % f.filename
            f.tags.append(t)
            
            t = Tag()
            t.name = "session"
            t.value = "session9"
            f.tags.append(t)
            
            files.append(f)
            
        self.client.add_subjects(files)


if __name__ == "__main__":
    unittest.main()
