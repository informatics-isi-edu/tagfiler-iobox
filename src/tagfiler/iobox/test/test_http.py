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

from tagfiler.iobox.models import RegisterTag, File
from tagfiler.util.http import TagfilerClient
import base

import unittest


class TagfilerAddAndFindSubjectsTest(base.OutboxBaseTestCase):
        
    def runTest(self):
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
        
        tagfiler_client = TagfilerClient(config=self.outbox_model.get_tagfiler())
        tagfiler_client.add_subject(register_file)
        
        result = tagfiler_client.find_subject_by_name(name)
        assert result is not None
        assert result[0]['name'] == name


class TagfilerAddSubjectsTest(base.OutboxBaseTestCase):
        
    def runTest(self):
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
        tagfiler_client = TagfilerClient(config=self.outbox_model.get_tagfiler())
        tagfiler_client.add_subjects(files)


if __name__ == "__main__":
    unittest.main()
