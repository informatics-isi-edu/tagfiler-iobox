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

from tagfiler.iobox.models import File, RegisterTag
from tagfiler.iobox.register import Register
from tagfiler.iobox import worker
from tagfiler.util.http import TagfilerClient
import base

import unittest
import random
import time


def all_tests():
    """Returns a TestSuite that includes all test cases in this module."""
    suite = unittest.TestSuite()
    suite.addTest(RegisterTest())
    return suite


class RegisterTest(base.OutboxBaseTestCase):

    def runTest(self):
        f = File()
        f.set_filepath("/home/smithd/test_register/test_%i" % random.random())
        f.set_size(100)
        f.set_checksum("mlmtrtekntlrkentlerter943t3493jt")
        self.state_dao.add_file(f)
        r = self.state_dao.register_file(f)
        t = RegisterTag()
        t.set_tag_name("name")
        t.set_tag_value("file://smithd#tagfiler_ep%s" % f.get_filepath())
        self.state_dao.add_tag_to_registered_file(r, t)
        t = RegisterTag()
        t.set_tag_name("session")
        t.set_tag_value("session9")
        self.state_dao.add_tag_to_registered_file(r, t)
        
        register_q = worker.WorkQueue()
        register_q.put(r)
        finish_q = worker.WorkQueue()
        
        register = Register(register_q, finish_q, self.state_dao, 
                            self.outbox_model.get_tagfiler())
        register.start()
        register_q.join()
        register.terminate()
        
        time.sleep(1)
        assert register_q.qsize() == 0
        assert finish_q.qsize() == 1
        
        tagfiler_client = TagfilerClient(config=self.outbox_model.get_tagfiler())
        result = tagfiler_client.find_subject_by_name(r.get_tag("name")[0].get_tag_value())
        assert result is not None


if __name__ == "__main__":
    unittest.main()
