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
queue of root directories. Each root directory is walked and file entries 
are filtered according to inclusion and exclusion patterns. It also gets the 
stats for each file entrye. The 'Find' stage then fills a queue with File 
model objects.
"""

import tagfiler.iobox.worker as worker
from tagfiler.util.files import tree_scan_stats, create_uri_friendly_file_path
from tagfiler.iobox.models import File
import outbox

import logging

logger = logging.getLogger(__name__)

class Find(worker.Worker):
    """The worker thread for the 'Find' stage of the Tagfiler Outbox."""
    
    def __init__(self, tasks, results, excludes=[], includes=[]):
        """Initializes the Find object.
        
        The 'tasks' parameter is a WorkQueue of pending tasks for the Find
        worker to process.
        
        The 'results' parameter is a WorkQueue of completed tasks output by
        the Find worker.
        
        The 'includes' and 'excludes' parameters are lists of re objects or 
        other objects that provide similar search(...) functions.
        """
        super(Find, self).__init__(tasks, results)
        self._includes = includes
        self._excludes = excludes
        
    def do_work(self, task, work_done):
        logger.debug('Find:do_work: %s' % task)
        if task is outbox.Outbox._FIND_DONE:
            work_done(task)
        else:
            path = task
            for (rfpath, size, mtime, user, group) in \
                    tree_scan_stats(path, self._excludes, self._includes):
                logger.debug('Find:do_work: scan: %s, %s, %s, %s, %s' % 
                             (rfpath, size, mtime, user, group))
                filename = create_uri_friendly_file_path(path, rfpath)
                args = {'filename': filename, 'mtime': mtime, 'size': size, \
                        'username': user, 'groupname': group}
                f = File(**args)
                work_done(f)
