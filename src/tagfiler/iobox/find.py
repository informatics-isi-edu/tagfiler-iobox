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
This module implements the 'Find' stage of the Tagfiler Outbox. It works on a 
queue of Root model objects. Each root directory is walked and file entries 
are filtered according to inclusion and exclusion patterns. It also gets the 
stats for each file entrye. The 'Find' stage then fills a queue with File 
model objects.
"""

import tagfiler.iobox.worker as worker
from tagfiler.util.files import tree_scan_stats, create_uri_friendly_file_path
from tagfiler.iobox.models import File
import logging

logger = logging.getLogger(__name__)

class Find(worker.Worker):
    """The worker thread for the 'Find' stage of the Tagfiler Outbox."""
    
    def __init__(self, tasks, results, inclusion_patterns=[], exclusion_patterns=[]):
        """
        Initializes the Find class.
        
        Arguments:
            tasks: a WorkQueue of tasks.
            results: a WorkQueue of results.
            inclusion_patterns: optional list of 'InclusionPattern' objects
            exclusion_patterns: optional list of 'ExclusionPattern' objects
        """
        super(Find, self).__init__(tasks, results)
        self._inclusion_patterns = inclusion_patterns
        self._exclusion_patterns = exclusion_patterns
        
    def do_work(self, task, work_done):
        path = task.get_filepath()
        logger.debug('Find:do_work: root: %s' % path)
        for (rfpath, size, mtime, user, group) in tree_scan_stats(path):
            logger.debug('Find:do_work: scan: %s, %s, %s, %s, %s' % 
                         (rfpath, size, mtime, user, group))
            filename = create_uri_friendly_file_path(path, rfpath)
            args = {'filename': filename, 'mtime': mtime, 'size': size, \
                    'username': user, 'groupname': group}
            f = File(**args)
            work_done(f)
