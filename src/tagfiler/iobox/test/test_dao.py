'''
Created on Sep 10, 2012

@author: smithd
'''
# test units
import unittest
import os
from tagfiler.iobox.models import *
from tagfiler.iobox.dao import OutboxDAO

class TestOutboxDAO(unittest.TestCase):
    
    def setUp(self):
        import tempfile
        p = {'outbox_name':'test_outbox', 'tagfiler_url':'https://jacoby.isi.edu/tagfiler', 'tagfiler_username':'smithd', 'tagfiler_password':'smithd'}
        outbox_file = os.path.join(tempfile.gettempdir(), "outbox.db")
        if os.path.exists(outbox_file):
            os.remove(outbox_file)
        self.dao = OutboxDAO(outbox_file, **p)
    def tearDown(self):
        self.dao.close()

    def testConstructor(self):
        pass

    def testGetOutboxByName(self):
        outbox = self.dao.get_outbox_by_name('test_outbox')
        assert outbox is not None
    def testAddRoot(self):
        outbox = self.dao.get_outbox_by_name('test_outbox')
        assert outbox.get_roots() is not None and len(outbox.get_roots()) == 0
        r1 = Root()
        r1.set_filename("/home/smithd/dir1/")
        r2 = Root()
        r2.set_filename("/home/smithd/dir2")
        
        roots = [r1, r2]
        for r in roots:
            self.dao.add_root_to_outbox(outbox, r)
        assert len(outbox.get_roots()) == 2
        for r in outbox.get_roots():
            assert r.get_id() is not None
        outbox = self.dao.get_outbox_by_name('test_outbox')
        assert outbox.get_roots() is not None and len(outbox.get_roots()) == 2
    def testAddExclusionPattern(self):
        outbox = self.dao.get_outbox_by_name('test_outbox')
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
        outbox = self.dao.get_outbox_by_name('test_outbox')
        assert outbox.get_exclusion_patterns() is not None and len(outbox.get_exclusion_patterns()) == 2

    def testAddInclusionPattern(self):
        outbox = self.dao.get_outbox_by_name('test_outbox')
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
        outbox = self.dao.get_outbox_by_name('test_outbox')
        assert outbox.get_inclusion_patterns() is not None and len(outbox.get_inclusion_patterns()) == 2
    def testAddPathMatch(self):
        outbox = self.dao.get_outbox_by_name('test_outbox')
        assert outbox.get_path_matches() is not None and len(outbox.get_path_matches()) == 0
        p1 = PathMatch()
        p1.set_name("assign directory tags")
        p1.set_pattern('^/.*/studies/([^/]+)/([^/]+)/')
        p1.set_extract("positional")
        t1 = PathMatchTag()
        t1.set_tag_name("date")
        t2 = PathMatchTag()
        t2.set_tag_name("session")
        p1.add_tag(t1)
        p1.add_tag(t2)
        tmpl = PathMatchTemplate()
        tmpl.set_template("some kind of template <1> and <2>")
        p1.add_template(tmpl)
        self.dao.add_path_match_to_outbox(outbox, p1)
        assert len(outbox.get_path_matches()) == 1
        for p in outbox.get_path_matches():
            assert p.get_id() is not None
        outbox = self.dao.get_outbox_by_name('test_outbox')
        assert outbox.get_path_matches() is not None and len(outbox.get_path_matches()) == 1
    
    def testAddLineMatch(self):
        outbox = self.dao.get_outbox_by_name('test_outbox')
        assert outbox.get_line_matches() is not None and len(outbox.get_line_matches()) == 0
        l1 = LineMatch()
        l1.set_name("test line rule")
        rp = PathRule()
        rp.set_pattern("^/.*studies")
        l1.set_path_rule(rp)
        lr = LineRule()
        lr.set_pattern("^,match,([0-9]+).*")
        lr.set_apply('match')
        lr.set_extract('positional')
        lrp = LineRulePrepattern()
        lrp.set_pattern(".*")
        lr.set_prepattern(lrp)
        l1.add_line_rule(lr)
        
        self.dao.add_line_match_to_outbox(outbox, l1)
        assert len(outbox.get_line_matches()) == 1
        for l in outbox.get_line_matches():
            assert l.get_id() is not None
        outbox = self.dao.get_outbox_by_name('test_outbox')
        assert outbox.get_line_matches() is not None and len(outbox.get_line_matches()) == 1

class TestOutboxInstanceDAO(unittest.TestCase):
    def setUp(self):
        import tempfile
        p = {'outbox_name':'test_outbox', 'tagfiler_url':'https://jacoby.isi.edu/tagfiler', 'tagfiler_username':'smithd', 'tagfiler_password':'smithd'}
        outbox_file = os.path.join(tempfile.gettempdir(), "outbox.db")
        if os.path.exists(outbox_file):
            os.remove(outbox_file)
        outbox_dao = OutboxDAO(outbox_file, **p)
        self.dao = outbox_dao.get_instance(outbox_dao.get_outbox_by_name('test_outbox'))

    def tearDown(self):
        self.dao.close()

    def testStartOutboxFileScan(self):
        scan = self.dao.start_file_scan()
        assert scan is not None
        assert len(self.dao.get_all_scans()) == 1
        
if __name__ == "__main__":
    unittest.main()
