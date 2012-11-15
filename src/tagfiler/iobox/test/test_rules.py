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

from tagfiler.iobox.models import File
from tagfiler.util.rules import PathRuleProcessor, TagDirector
from tagfiler.iobox.test.base import create_date_and_study_path_rule
from tagfiler.iobox.models import create_default_name_path_rule
import socket
import unittest
import logging

def all_tests():
    """Returns a TestSuite that includes all test cases in this module."""
    suite = unittest.TestSuite()
    suite.addTest(TestPathRuleProcessor())
    suite.addTest(TestTagDirector())
    return suite


class TestPathRuleProcessor(unittest.TestCase):
    
    def testRun(self):
        path_rule = create_date_and_study_path_rule()
        processor = PathRuleProcessor(path_rule)
        result = processor.analyze("/opt/data/studies/2012-02-23/session1/myfile.jpg")
        assert result.get('date').pop() == '2012-02-23'
        assert result.get('session').pop() == 'session1'

        path_rule = create_default_name_path_rule(socket.getfqdn())
        processor = PathRuleProcessor(path_rule)
        result = processor.analyze("/opt/data/studies/2012-02-23/session1/myfile.jpg")
        assert result.get('name').pop() == "file://%s/opt/data/studies/2012-02-23/session1/myfile.jpg" % socket.getfqdn()
        
class TestTagDirector(unittest.TestCase):
    def testRun(self):
        f = File()
        f.filename = "/opt/data/studies/2012-02-23/session1/myfile.jpg"
        
        rules = [create_date_and_study_path_rule(), create_default_name_path_rule(socket.gethostname())]
        TagDirector().tag_registered_file(rules, f)
        assert len(f.tags) == 3
        for t in f.tags:
            if t.name == "date":
                assert t.value == '2012-02-23'
            elif t.name == "session":
                assert t.value == "session1"
            elif t.name == "name":
                assert t.value == "file://%s/opt/data/studies/2012-02-23/session1/myfile.jpg" % socket.gethostname()
            else:
                assert False

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()