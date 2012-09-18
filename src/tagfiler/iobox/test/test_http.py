'''
Created on Sep 17, 2012

@author: smithd
'''
import unittest
import os
from tagfiler.iobox.test.test_dao import create_test_outbox
from tagfiler.iobox.dao import OutboxDAO
from tagfiler.iobox.models import RegisterTag, File
from tagfiler.util.http import TagfilerClient

class TestTagfilerClient(unittest.TestCase):
    def setUp(self):
        import tempfile
        outbox_file = os.path.join(tempfile.gettempdir(), "outbox.db")
        if os.path.exists(outbox_file):
            os.remove(outbox_file)
        self.dao = OutboxDAO(outbox_file)
        self.outbox= create_test_outbox()
        self.dao.add_outbox(self.outbox)
        self.state_dao=self.dao.get_state_dao(self.outbox)
        
    def tearDown(self):
        self.state_dao.close()
        self.dao.close()
        
    def testAddAndFindSubject(self):
        import random
        f = File()
        f.set_filepath("/home/smithd/tagfiler_test%s.jpg" % unicode(random.random()))
        f.set_size(100)
        self.state_dao.add_file(f)
        register_file = self.state_dao.register_file(f)
        t = RegisterTag()
        t.set_tag_name("name")
        name = "file://smithd#tagfiler_ep%s" % f.get_filepath()
        t.set_tag_value(name)
        self.state_dao.add_tag_to_registered_file(register_file, t)
        t = RegisterTag()
        t.set_tag_name("session")
        t.set_tag_value("session9")
        self.state_dao.add_tag_to_registered_file(register_file, t)
        t = RegisterTag()
        t.set_tag_name("sha256sum")
        t.set_tag_value("53534mnl5k34n5l34kn5")
        self.state_dao.add_tag_to_registered_file(register_file, t)
        
        tagfiler_client = TagfilerClient(config=self.outbox.get_tagfiler())
        tagfiler_client.add_subject(register_file)
        
        result = tagfiler_client.find_subject_by_name(name)
        assert result is not None
        assert result[0]['name'] == name

    def testAddSubjects(self):
        import random
        files = []
        for i in range(1, 10):
            f = File()
            f.set_filepath("/home/smithd/tagfiler_test%s.jpg" % unicode(random.random()))
            f.set_size(100)
            self.state_dao.add_file(f)
            register_file = self.state_dao.register_file(f)
            t = RegisterTag()
            t.set_tag_name("name")
            t.set_tag_value("file://smithd#tagfiler_ep%s" % f.get_filepath())
            self.state_dao.add_tag_to_registered_file(register_file, t)
            t = RegisterTag()
            t.set_tag_name("session")
            t.set_tag_value("session9")
            self.state_dao.add_tag_to_registered_file(register_file, t)
            files.append(register_file)
        tagfiler_client = TagfilerClient(config=self.outbox.get_tagfiler())
        tagfiler_client.add_subjects(files)
if __name__ == "__main__":
    unittest.main()