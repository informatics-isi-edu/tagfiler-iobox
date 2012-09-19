'''
Created on Sep 19, 2012

@author: smithd
'''
import unittest
import logging
from tagfiler.iobox.models import PathRule, RERuleTag, RERuleTemplate, File, RegisterFile
from tagfiler.util.rules import PathRuleProcessor, TagDirector

test_endpoint_name = "smithd#tagfiler_ep"
def _create_date_and_study_path_rule():
    path_rule = PathRule()
    path_rule.set_pattern('^/.*/studies/([^/]+)/([^/]+)/')
    path_rule.set_extract('positional')
    date_tag = RERuleTag()
    date_tag.set_tag_name('date')
    session_tag = RERuleTag()
    session_tag.set_tag_name('session')
    path_rule.set_tags([date_tag, session_tag])
        
    return path_rule

def _create_name_path_rule():
    path_rule = PathRule()
    path_rule.set_pattern('^(?P<path>.*)')
    path_rule.set_extract('template')
    t1 = RERuleTemplate()
    t1.set_template('file://%s\g<path>' % test_endpoint_name)
    path_rule.add_template(t1)
    tg1 = RERuleTag()
    tg1.set_tag_name('name')
    path_rule.add_tag(tg1)
    
    return path_rule

class TestPathRuleProcessor(unittest.TestCase):
    
    def testAnalyze(self):
        path_rule = _create_date_and_study_path_rule()
        processor = PathRuleProcessor(path_rule)
        result = processor.analyze("/opt/data/studies/2012-02-23/session1/myfile.jpg")
        assert result.get('date').pop() == '2012-02-23'
        assert result.get('session').pop() == 'session1'

        path_rule = _create_name_path_rule()
        processor = PathRuleProcessor(path_rule)
        result = processor.analyze("/opt/data/studies/2012-02-23/session1/myfile.jpg")
        assert result.get('name').pop() == "file://%s/opt/data/studies/2012-02-23/session1/myfile.jpg" % test_endpoint_name
        
class TestTagDirector(unittest.TestCase):
    def testTagRegisteredFile(self):
        register_file = RegisterFile()
        f = File()
        f.set_filepath("/opt/data/studies/2012-02-23/session1/myfile.jpg")
        register_file.set_file(f)
        
        rules = [_create_date_and_study_path_rule(), _create_name_path_rule()]
        TagDirector().tag_registered_file(rules, register_file)
        assert len(register_file.get_tags()) == 3
        for t in register_file.get_tags():
            if t.get_tag_name() == "date":
                assert t.get_tag_value() == '2012-02-23'
            elif t.get_tag_name() == "session":
                assert t.get_tag_value() == "session1"
            elif t.get_tag_name() == "name":
                assert t.get_tag_value() == "file://%s/opt/data/studies/2012-02-23/session1/myfile.jpg" % test_endpoint_name
            else:
                assert False

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()