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
from tagfiler.iobox.models import Root, File
from tagfiler.iobox.dao import OutboxStateDAO
import logging

logger = logging.getLogger(__name__)

class Find(worker.Worker):
    """The worker thread for the 'Find' stage of the Tagfiler Outbox."""
    
    def __init__(self, tasks, results, state_dao, inclusion_patterns=[], exclusion_patterns=[]):
        """
        Initializes the Find class.
        
        Arguments:
            tasks: a WorkQueue of tasks.
            results: a WorkQueue of results.
            inclusion_patterns: optional list of 'InclusionPattern' objects
            exclusion_patterns: optional list of 'ExclusionPattern' objects
        """
        super(Find, self).__init__(tasks, results)
        assert isinstance(state_dao, OutboxStateDAO)
        self._state_dao = state_dao
        self._inclusion_patterns = inclusion_patterns
        self._exclusion_patterns = exclusion_patterns
        
    def do_work(self, task, work_done):
        assert isinstance(task, Root)
        root = task # We expect task to be of type models.Root
        path = root.get_filepath()
        logger.debug('Find:do_work: path: %s' % path)
        for (rfpath, size, mtime, user, group) in tree_scan_stats(path):
            logger.debug('Find:do_work: scan: %s, %s, %s, %s, %s' % 
                         (rfpath, size, mtime, user, group))
            filepath = create_uri_friendly_file_path(path, rfpath)
            f = self._state_dao.find_file_by_path(filepath)
            if f is None:
                f = File(filepath=filepath, size=size, mtime=mtime, 
                     user=user, group=group, must_tag=True)
                self._state_dao.add_file(f)
                work_done(f)
            else:
                # determine if the file has changed since the last scan
                if size != f.get_size():
                    f.set_size(size)
                    f.set_mtime(mtime)
                    f.set_must_tag(True)
                    self._state_dao.update_file(f)
                    work_done(f)
                elif mtime > f.get_mtime():
                    # TODO: compute checksum of file and compare.
                    # If the checksums differ, update for tagging
                    pass
           
