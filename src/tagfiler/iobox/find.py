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
Placeholder for the find module.
"""

import logging
import worker
import tagfiler.util.files as fileutil

logger = logging.getLogger(__name__)

class Find(worker.Worker):
    """A worker for performing the find stage of the outbox pipeline."""
    
    def __init__(self, tasks, results, inclusion_patterns=[], exclusion_patterns=[]):
        """
        Initializes the Find class.
        
        Arguments:
            tasks: a WorkQueue of tasks.
            results: a WorkQueue of results.
            inclusion_patterns: optional 'InclusionPattern' list
            exclusion_patterns: optional 'ExclusionPattern' list
        """
        worker.Worker.__init__(self, tasks, results)
        self._inclusion_patterns = inclusion_patterns
        self._exclusion_patterns = exclusion_patterns
        
    def do_work(self, task, work_done):
        path = task.get_filepath() # We expect task to be of type models.Root
        logger.debug('Find:do_work: path: %s' % path)
        for fname in fileutil.tree_scan(path):
            logger.debug('Find:do_work: file: %s' % fname)
            work_done(fname)
