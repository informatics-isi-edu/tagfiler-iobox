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

import tagfiler.iobox.worker as worker
from tagfiler.util.files import tree_scan_stats
from tagfiler.iobox.models import File

import logging

logger = logging.getLogger(__name__)

class Find(worker.Worker):
    """A worker for performing the find stage of the outbox pipeline."""
    
    def __init__(self, tasks, results, inclusion_patterns=[], exclusion_patterns=[]):
        """
        Initializes the Find class.
        
        Arguments:
            tasks: a WorkQueue of tasks.
            results: a WorkQueue of results.
            inclusion_patterns: optional list of 'InclusionPattern' objects
            exclusion_patterns: optional list of 'ExclusionPattern' objects
        """
        worker.Worker.__init__(self, tasks, results)
        self._inclusion_patterns = inclusion_patterns
        self._exclusion_patterns = exclusion_patterns
        
    def do_work(self, task, work_done):
        root = task # We expect task to be of type models.Root
        path = root.get_filepath()
        logger.debug('Find:do_work: path: %s' % path)
        for (rfpath, size, mtime, user, group) in tree_scan_stats(path):
            logger.debug('Find:do_work: scan: %s, %s, %s, %s, %s' % 
                         (rfpath, size, mtime, user, group))
            f = File(filepath=rfpath, size=size, mtime=mtime, 
                     user=user, group=group, must_tag=True)
            work_done(f)
