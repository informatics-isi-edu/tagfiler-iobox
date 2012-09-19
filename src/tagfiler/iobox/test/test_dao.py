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
Unit tests for the dao module.
"""

from tagfiler.iobox.models import *
from tagfiler.iobox.dao import OutboxDAO

import unittest
import os
import time
import logging


logger = logging.getLogger(__name__)


def all_tests():
    suite = unittest.TestSuite()
    # Add test cases to suite
    return suite


def create_test_outbox():
    outbox = Outbox()
    outbox.set_name('test_outbox')
    tagfiler = Tagfiler()
    tagfiler.set_url('https://jacoby.isi.edu/tagfiler')
    tagfiler.set_username('smithd')
    tagfiler.set_password('smithd')
    outbox.set_tagfiler(tagfiler)
    return outbox

class TestOutboxDAO(unittest.TestCase):
    
    def setUp(self):
        import tempfile
        outbox_file = os.path.join(tempfile.gettempdir(), "outbox.db")
        if os.path.exists(outbox_file):
            os.remove(outbox_file)
        self.dao = OutboxDAO(outbox_file)
        self.dao.add_outbox(create_test_outbox())

    def tearDown(self):
        self.dao.close()

    def testConstructor(self):
        pass

    def testGetOutboxByName(self):
        outbox = self.dao.find_outbox_by_name('test_outbox')
        assert outbox is not None
    def testAddRoot(self):
        outbox = self.dao.find_outbox_by_name('test_outbox')
        assert outbox.get_roots() is not None and len(outbox.get_roots()) == 0
        r1 = Root()
        r1.set_filepath("/home/smithd/dir1/")
        r2 = Root()
        r2.set_filepath("/home/smithd/dir2")
        
        roots = [r1, r2]
        for r in roots:
            self.dao.add_root_to_outbox(outbox, r)
        assert len(outbox.get_roots()) == 2
        for r in outbox.get_roots():
            assert r.get_id() is not None
        outbox = self.dao.find_outbox_by_name('test_outbox')
        assert outbox.get_roots() is not None and len(outbox.get_roots()) == 2
    def testAddExclusionPattern(self):
        outbox = self.dao.find_outbox_by_name('test_outbox')
        assert outbox.get_exclusion_patterns() is not None and len(outbox.get_exclusion_patterns()) == 0
        e1 = ExclusionPattern()
        e1.set_pattern("/.*/")
        e2 = ExclusionPattern()
        e2.set_pattern("/^[a-w]+/")
        patterns = [e1, e2]
        for p in patterns:
            self.dao.add_exclusion_pattern_to_outbox(outbox, p)
        assert len(outbox.get_exclusion_patterns()) == 2
        for p in outbox.get_exclusion_patterns():
            assert p.get_id() is not None
        outbox = self.dao.find_outbox_by_name('test_outbox')
        assert outbox.get_exclusion_patterns() is not None and len(outbox.get_exclusion_patterns()) == 2

    def testAddInclusionPattern(self):
        outbox = self.dao.find_outbox_by_name('test_outbox')
        assert outbox.get_inclusion_patterns() is not None and len(outbox.get_inclusion_patterns()) == 0
        e1 = InclusionPattern()
        e1.set_pattern("/.*/")
        e2 = InclusionPattern()
        e2.set_pattern("/^[a-w]+/")
        patterns = [e1, e2]
        for p in patterns:
            self.dao.add_inclusion_pattern_to_outbox(outbox, p)
        assert len(outbox.get_inclusion_patterns()) == 2
        for p in outbox.get_inclusion_patterns():
            assert p.get_id() is not None
        outbox = self.dao.find_outbox_by_name('test_outbox')
        assert outbox.get_inclusion_patterns() is not None and len(outbox.get_inclusion_patterns()) == 2
    def testAddPathRule(self):
        outbox = self.dao.find_outbox_by_name('test_outbox')
        assert outbox.get_path_rules() is not None and len(outbox.get_path_rules()) == 0
        pattern_str = '^/.*/studies/([^/]+)/([^/]+)/'
        name_str = "assign directory tags"
        extract_str = "positional"
        tag1_str = "date"
        tag2_str = "session"
        template_str = "some kind of template <1> and <2>"
        rewrite_pattern_str = ".*"
        rewrite_template_str = "<hello>"
        constant_name_str = "test"
        constant_value_str = "hello"
        apply_str = "template"
        
        p1 = PathRule()
        p1.set_name(name_str)
        p1.set_pattern(pattern_str)
        p1.set_extract(extract_str)
        p1.set_apply(apply_str)
        t1 = RERuleTag()
        t1.set_tag_name(tag1_str)
        t2 = RERuleTag()
        t2.set_tag_name(tag2_str)
        p1.add_tag(t1)
        p1.add_tag(t2)
        tmpl = RERuleTemplate()
        tmpl.set_template(template_str)
        p1.add_template(tmpl)
        r1 = RERuleRewrite()
        r1.set_rewrite_pattern(rewrite_pattern_str)
        r1.set_rewrite_template(rewrite_template_str)
        p1.add_rewrite(r1)
        c1 = RERuleConstant()
        c1.set_constant_name(constant_name_str)
        c1.set_constant_value(constant_value_str)
        p1.add_constant(c1)
        
        self.dao.add_path_rule_to_outbox(outbox, p1)
        assert len(outbox.get_path_rules()) == 1
        for p in outbox.get_path_rules():
            assert p.get_id() is not None
        outbox = self.dao.find_outbox_by_name('test_outbox')
        assert outbox.get_path_rules() is not None and len(outbox.get_path_rules()) == 1
        path_rule = outbox.get_path_rules()[0]
        assert path_rule.get_pattern() == pattern_str
        assert path_rule.get_extract() == extract_str
        assert path_rule.get_apply() == apply_str
        assert len(path_rule.get_tags()) == 2 and path_rule.get_tags()[0].get_tag_name() == tag1_str
        assert len(path_rule.get_templates()) == 1 and path_rule.get_templates()[0].get_template() == template_str
        assert len(path_rule.get_rewrites()) == 1 and path_rule.get_rewrites()[0].get_rewrite_pattern() == rewrite_pattern_str
        assert len(path_rule.get_constants()) == 1 and path_rule.get_constants()[0].get_constant_name() == constant_name_str

    def testAddLineRule(self):
        outbox = self.dao.find_outbox_by_name('test_outbox')
        assert outbox.get_line_rules() is not None and len(outbox.get_line_rules()) == 0
        pattern_str = '^/.*/studies/([^/]+)/([^/]+)/'
        name_str = "assign directory tags"
        extract_str = "positional"
        tag1_str = "date"
        tag2_str = "session"
        template_str = "some kind of template <1> and <2>"
        rewrite_pattern_str = ".*"
        rewrite_template_str = "<hello>"
        constant_name_str = "test"
        constant_value_str = "hello"
        apply_str = "template"
        
        l1 = LineRule()
        l1.set_name("test line rule")
        rp = PathRule()
        rp.set_pattern("^/.*studies")
        l1.set_path_rule(rp)
        for i in range(0, 10):
            p1 = RERule()
            p1.set_name(name_str)
            p1.set_pattern(pattern_str)
            p1.set_extract(extract_str)
            p1.set_apply(apply_str)
            t1 = RERuleTag()
            t1.set_tag_name(tag1_str)
            t2 = RERuleTag()
            t2.set_tag_name(tag2_str)
            p1.add_tag(t1)
            p1.add_tag(t2)
            tmpl = RERuleTemplate()
            tmpl.set_template(template_str)
            p1.add_template(tmpl)
            r1 = RERuleRewrite()
            r1.set_rewrite_pattern(rewrite_pattern_str)
            r1.set_rewrite_template(rewrite_template_str)
            p1.add_rewrite(r1)
            c1 = RERuleConstant()
            c1.set_constant_name(constant_name_str)
            c1.set_constant_value(constant_value_str)
            p1.add_constant(c1)
            l1.add_rerule(p1)
        
        self.dao.add_line_rule_to_outbox(outbox, l1)
        assert len(outbox.get_line_rules()) == 1
        for l in outbox.get_line_rules():
            assert l.get_id() is not None
        outbox = self.dao.find_outbox_by_name('test_outbox')
        assert outbox.get_line_rules() is not None and len(outbox.get_line_rules()) == 1
        line_rule = outbox.get_line_rules()[0]
        assert line_rule.get_path_rule() is not None
        assert len(line_rule.get_rerules()) == 10
        assert len(line_rule.get_rerules()[0].get_tags()) == 2
        assert len(line_rule.get_rerules()[0].get_constants()) == 1
        assert len(line_rule.get_rerules()[0].get_rewrites()) == 1
        assert len(line_rule.get_rerules()[0].get_templates()) == 1
        assert line_rule.get_name() == "test line rule"
        
        l2 = LineRule()
        self.dao.add_line_rule_to_outbox(outbox, l2)
        assert l2.get_id() is not None
        outbox = self.dao.find_outbox_by_name('test_outbox')
        assert len(outbox.get_line_rules()) == 2

class TestOutboxStateDAO(unittest.TestCase):
    def setUp(self):
        import tempfile
        outbox_file = os.path.join(tempfile.gettempdir(), "outbox.db")
        outbox_instance_file = os.path.join(tempfile.gettempdir(), "outbox_1.db")
        if os.path.exists(outbox_file):
            os.remove(outbox_file)
        if os.path.exists(outbox_instance_file):
            os.remove(outbox_instance_file)
        
        outbox_dao = OutboxDAO(outbox_file)
        outbox = create_test_outbox()
        outbox_dao.add_outbox(outbox)
        self.dao = outbox_dao.get_state_dao(outbox)

    def tearDown(self):
        self.dao.close()

    def testStartOutboxFileScan(self):
        scan = self.dao.start_file_scan()
        assert scan is not None
        assert len(self.dao.find_all_scans()) == 1
        
        scan2 = self.dao.start_file_scan()
        assert scan.get_id() != scan2.get_id()

    def testGetLastScan(self):
        last_scan = self.dao.find_last_scan()
        assert last_scan is None
        scan1 = self.dao.start_file_scan()
        scan2 = self.dao.find_last_scan()
        assert scan1.get_id() == scan2.get_id()

    def testAddAndGetFile(self):
        
        for i in range(0, 10):
            f = File()
            f.set_filepath("/home/smithd/myfile%i.txt" % i)
            f.set_mtime(time.time())
            f.set_size(6000)
            f.set_checksum("54359u34059u0g9jdgijdflgjkdlf%i" % i)
            self.dao.add_file(f)
            assert f.get_id() is not None

        for i in range(0,10):
            assert self.dao.find_file_by_path("/home/smithd/myfile%i.txt" % i) is not None

    def testUpdateFile(self):
        checksum1 = "aaaaaaaaaaaaaaaaaaaaaa"
        checksum2 = "bbbbbbbbbbbbbbbbbbbbbb"
        checksum1_a = "cccccccccccccccccccc"
        checksum2_a = "dddddddddddddddddddd"
        
        filename1 = "/home/smithd/myfile1.txt"
        filename2 = "/home/smithd/myfile2.txt"
        
        size1 = 600
        size2 = 800
        size1_a = 650
        size2_a = 850
        
        mtime1 = time.time()
        
        f1 = File()
        f1.set_filepath(filename1)
        f1.set_mtime(mtime1)
        f1.set_size(size1)
        f1.set_checksum(checksum1)
        
        f2 = File()
        f2.set_filepath(filename2)
        f2.set_mtime(mtime1)
        f2.set_size(size2)
        f2.set_checksum(checksum2)
        
        self.dao.add_file(f1)
        self.dao.add_file(f2)
        
        mtime2 = time.time()
        
        f1.set_checksum(checksum1_a)
        f1.set_mtime(mtime2)
        f1.set_size(size1_a)
        
        f2.set_checksum(checksum2_a)
        f2.set_mtime(mtime2)
        f2.set_size(size2_a)
        
        self.dao.update_file(f1)
        self.dao.update_file(f2)
        
        assert f1.get_checksum() == checksum1_a and f2.get_checksum() == checksum2_a
        assert f1.get_mtime() == mtime2 and f2.get_mtime() == mtime2
        assert f1.get_size() == size1_a and f2.get_size() == size2_a
        
        f1 = self.dao.find_file_by_path(filename1)
        f2 = self.dao.find_file_by_path(filename2)
        
        assert f1.get_checksum() == checksum1_a and f2.get_checksum() == checksum2_a
        assert f1.get_mtime() == mtime2 and f2.get_mtime() == mtime2
        assert f1.get_size() == size1_a and f2.get_size() == size2_a

    def testAddAndGetFilesInScans(self):
        scan1 = self.dao.start_file_scan()
        files = self.dao.find_files_in_scan(scan1)
        assert files is not None and len(files) == 0
        scan2 = self.dao.start_file_scan()
        for i in range(0,6):
            f = File()
            f.set_filepath("/home/smithd/set1/file%i.txt" % i)
            self.dao.add_file(f)
            self.dao.add_file_to_scan(scan1, f)
        for i in range(0,4):
            f = File()
            f.set_filepath("/home/smithd/set2/file%i.txt" % i)
            self.dao.add_file(f)
            self.dao.add_file_to_scan(scan2, f)
        assert len(self.dao.find_files_in_scan(scan1)) == 6
        assert len(self.dao.find_files_in_scan(scan2)) == 4

    def testFinishFileScan(self):
        scan = self.dao.start_file_scan()
        self.dao.finish_file_scan(scan)
        assert scan.get_state().get_state() == "COMPLETED_FILE_SCAN"

    def testGetScansToTag(self):
        assert len(self.dao.find_scans_to_tag()) == 0
        scan1 = self.dao.start_file_scan()
        scan2 = self.dao.start_file_scan()
        scan3 = self.dao.start_file_scan()
        
        f = File()
        f.set_filepath("/home/smithd/test1.txt")
        self.dao.add_file(f)
        self.dao.add_file_to_scan(scan1, f)
        f2 = File()
        f2.set_filepath("/home/smithd/test2.txt")
        self.dao.add_file(f2)
        self.dao.add_file_to_scan(scan2, f2)
        self.dao.finish_file_scan(scan1)
        self.dao.finish_file_scan(scan2)

        scans = self.dao.find_scans_to_tag()
        assert len(scans) == 2
        for scan in scans:
            assert len(scan.get_files()) == 1
        self.dao.finish_file_scan(scan3)
        assert len(self.dao.find_scans_to_tag()) == 3
        
    def testRegisterFile(self):
        f = File()
        f.set_filepath("/home/smithd/test1.txt")
        f2 = File()
        f2.set_filepath("/home/smithd/test2.txt")
        scan = self.dao.start_file_scan()
        self.dao.add_file(f)
        self.dao.add_file(f2)
        self.dao.add_file_to_scan(scan, f)
        self.dao.add_file_to_scan(scan, f2)
        r1 = self.dao.register_file(f)
        r2 = self.dao.register_file(f2)
        
        assert r1 is not None
        assert r2 is not None
        assert r1.get_id() != r2.get_id()
        
    def testGetAndAddTagToRegisteredFile(self):
        f = File()
        f.set_filepath("/home/smithd/test1.txt")
        f.set_must_tag(False)
        f2 = File()
        f2.set_filepath("/home/smithd/test2.txt")
        f2.set_must_tag(False)
        scan = self.dao.start_file_scan()
        self.dao.add_file(f)
        self.dao.add_file(f2)
        self.dao.add_file_to_scan(scan, f)
        self.dao.add_file_to_scan(scan, f2)
        r1 = self.dao.register_file(f)
        r2 = self.dao.register_file(f2)
        
        assert len(r1.get_tags()) == 0 and len(r2.get_tags()) == 0
        t1 = RegisterTag()
        t1.set_tag_name("tag1")
        t1.set_tag_value("hello")
        t2 = RegisterTag()
        t2.set_tag_name("tag2")
        t2.set_tag_value("goodbye")
        t3 = RegisterTag()
        t3.set_tag_name("tag1")
        t3.set_tag_value("world")
        t4 = RegisterTag()
        t4.set_tag_name("tag1")
        t4.set_tag_value("hello")
        
        self.dao.add_tag_to_registered_file(r1, t1)
        self.dao.add_tag_to_registered_file(r1, t2)
        self.dao.add_tag_to_registered_file(r1, t3)
        self.dao.add_tag_to_registered_file(r2, t4)
        
        assert len(r1.get_tags()) == 3 and len(r2.get_tags()) == 1
        files = self.dao.find_tagged_files_to_register()
        for f in files:
            assert len(f.get_tags()) > 1

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
