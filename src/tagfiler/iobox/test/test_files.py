'''
Created on Sep 26, 2012

@author: smithd
'''
import unittest
from tagfiler.util.files import create_uri_friendly_file_path
import logging

def all_tests():
    """Returns a TestSuite that includes all test cases in this module."""
    suite = unittest.TestSuite()
    suite.addTest(TestCreateUriFriendlyFilePath())
    return suite

class TestCreateUriFriendlyFilePath(unittest.TestCase):
    def runTest(self):
        win_path_orig_path = "c:\\Users\\smithd\\Documents\\test1\\"
        win_path_orig_name = "myfile.txt"
        win_path_converted = "/c:/Users/smithd/Documents/test1/myfile.txt"
        
        assert create_uri_friendly_file_path(win_path_orig_path, win_path_orig_name) == win_path_converted
        
        unix_path_orig_path = "/opt/data/studies/"
        unix_path_orig_name = "myfile.txt"
        unix_path_converted = "/opt/data/studies/myfile.txt"
        assert create_uri_friendly_file_path(unix_path_orig_path, unix_path_orig_name) == unix_path_converted
        
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()