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

from tagfiler.iobox.models import File
from tagfiler.iobox.tag import Tag
from tagfiler.iobox.test import base
from tagfiler.iobox import worker
from tagfiler.iobox.test.base import create_date_and_study_path_rule
from tagfiler.iobox.models import create_default_name_path_rule
from tagfiler.util.rules import TagDirector
import unittest
import logging
import time

def all_tests():
    """Returns a TestSuite that includes all test cases in this module."""
    suite = unittest.TestSuite()
    suite.addTest(TagTest())
    return suite


class TagTest(base.OutboxBaseTestCase):
    
    def runTest(self):
        f = File()
        f.filename = "/opt/data/studies/2012-05-23/session1/myfile.jpg"
        f.size = 100
        f.checksum = "mlmtrtekntlrkentlerter943t3493jt"
        
        tag_q = worker.WorkQueue()
        tag_q.put(f)
        finish_q = worker.WorkQueue()
        
        all_rules = [create_date_and_study_path_rule(), create_default_name_path_rule('localhost')]
        
        tag = Tag(tag_q, finish_q, all_rules, TagDirector())
        tag.start()
        tag_q.join()
        tag.terminate()
        
        time.sleep(1)
        assert tag_q.qsize() == 0
        assert finish_q.qsize() == 1
        tagged_file = finish_q.get_nowait()
        assert len(tagged_file.tags) == 3

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
