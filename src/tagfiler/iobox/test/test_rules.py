'''
Created on Sep 19, 2012

@author: smithd
'''
import unittest
import logging
from tagfiler.iobox.models import File
from tagfiler.util.rules import PathRuleProcessor, TagDirector
from tagfiler.iobox.test import base
from tagfiler.iobox.test.base import create_date_and_study_path_rule
from tagfiler.iobox.models import create_default_name_path_rule
import socket

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
        f.set_filepath("/opt/data/studies/2012-02-23/session1/myfile.jpg")
        
        rules = [create_date_and_study_path_rule(), create_default_name_path_rule(socket.gethostname())]
        TagDirector().tag_registered_file(rules, f)
        assert len(f.get_tags()) == 3
        for t in f.get_tags():
            if t.get_tag_name() == "date":
                assert t.get_tag_value() == '2012-02-23'
            elif t.get_tag_name() == "session":
                assert t.get_tag_value() == "session1"
            elif t.get_tag_name() == "name":
                assert t.get_tag_value() == "file://%s/opt/data/studies/2012-02-23/session1/myfile.jpg" % socket.gethostname()
            else:
                assert False

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()