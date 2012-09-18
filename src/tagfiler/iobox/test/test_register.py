'''
Created on Sep 17, 2012

@author: smithd
'''
import unittest
import random
from tagfiler.iobox.models import File, RegisterTag
from tagfiler.iobox.dao import OutboxDAO
from tagfiler.iobox.test.test_dao import create_test_outbox
from tagfiler.iobox.register import Register
from tagfiler.iobox import worker
from tagfiler.util.http import TagfilerClient

import os

class TestRegister(unittest.TestCase):
    def setUp(self):
        import tempfile
        outbox_file = os.path.join(tempfile.gettempdir(), "outbox.db")
        if os.path.exists(outbox_file):
            os.remove(outbox_file)
        self.dao = OutboxDAO(outbox_file)
        self.outbox = create_test_outbox()
        self.dao.add_outbox(self.outbox)
        self.state_dao = self.dao.get_state_dao(self.outbox)
        
        
        
    def tearDown(self):
        self.dao.close()
        self.state_dao.close()

    def testDoWork(self):
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
        
        register = Register(register_q, finish_q, config=self.outbox.get_tagfiler())
        register.start()
        register_q.join()
        register.terminate()
        
        assert register_q.qsize() == 0
        assert finish_q.qsize() == 1
        
        tagfiler_client = TagfilerClient(config=self.outbox.get_tagfiler())
        result = tagfiler_client.find_subject_by_name(r.get_tag("name")[0].get_tag_value())
        assert result is not None
        
if __name__ == "__main__":
    unittest.main()
