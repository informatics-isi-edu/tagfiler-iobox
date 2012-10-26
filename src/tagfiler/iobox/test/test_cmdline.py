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
Unit tests for cmdline module.
"""

import unittest
import base
from tagfiler.iobox import cmdline


class CmdlineTest(unittest.TestCase):
    """Commandline test cases.
    
    We do things a little different for testing the cmdline module, because 
    the cmdline.main() sets up its temporary DAO and Outbox model. We do not 
    need to inherit from the usual base.OutboxBaseTestCase.
    """

    def setUp(self):
        self.rootdirs = base.create_temp_dirtree(1, 1, 5)
        self.args = ['--url=https://curiosity.isi.edu/tagfiler', '--username=demo', '--password=demo'] #TODO(rs):need to fix
        for rootdir in self.rootdirs:
            self.args.extend(['--rootdir=%s' % rootdir])

    def tearDown(self):
        base.remove_temp_dirtree(self.rootdirs)

    def testBaseline(self):
        cmdline.main(self.args)

    def testExclude(self):
        args = ['--exclude=.*'] #TODO(rs): placeholder
        args.extend(self.args)
        cmdline.main(args)
        
    def testInclude(self):
        args = ['--include=.*'] #TODO(rs): placeholder
        args.extend(self.args)
        cmdline.main(args)

    def testIncludeExclude(self):
        args = ['--exclude=.*', '--include=.*'] #TODO(rs): placeholder
        args.extend(self.args)
        cmdline.main(args)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()