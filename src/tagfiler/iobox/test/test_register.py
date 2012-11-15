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
Unit tests for register module.
"""

from tagfiler.iobox.models import File, Tag
from tagfiler.iobox.register import Register
from tagfiler.iobox.worker import WorkQueue
from tagfiler.iobox.test.base import create_test_outbox
from tagfiler.util.http import TagfilerClient

import unittest
import random
import time
import logging


logger = logging.getLogger(__name__)


def all_tests():
    """Returns a TestSuite that includes all test cases in this module."""
    suite = unittest.TestSuite()
    suite.addTest(RegisterTest())
    return suite


class RegisterTest(unittest.TestCase):

    def setUp(self):
        outbox_model = create_test_outbox()
        self.client = TagfilerClient(outbox_model.url, outbox_model.username, 
                                     outbox_model.password)
        self.client.connect()
        self.client.login()
    
    def tearDown(self):
        self.client.close()

    def runTest(self):
        f = File()
        f.filename = "/home/smithd/test_register/test_%i" % random.random()
        f.size = 100
        f.checksum = "mlmtrtekntlrkentlerter943t3493jt"
        
        t = Tag()
        t.name = "name"
        t.value = "file://demo#tagfiler_ep%s" % f.filename
        f.tags.append(t)
        t = Tag()
        t.name = "session"
        t.value = "session9"
        f.tags.append(t)
        
        register_q = WorkQueue()
        register_q.put(f)
        finish_q = WorkQueue()
        
        register = Register(register_q, finish_q, self.client)
        register.start()
        register_q.join()
        register.terminate()
        time.sleep(1)
        result = self.client.find_subject_by_name(f.filter_tags("name")[0].value)
        
        assert register_q.qsize() == 0
        assert finish_q.qsize() == 1
        assert result is not None


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
