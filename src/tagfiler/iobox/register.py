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
Implements the registration stage of the Outbox.
"""

from worker import Worker
from models import File
import outbox

import time
import logging


logger = logging.getLogger(__name__)


class Register(Worker):
    """The registration pipeline worker."""
    
    def __init__(self, tasks, results, client, bulk_ops_max=0):
        super(Register, self).__init__(tasks, results)
        self._client = client
        self._bulk_ops_max = bulk_ops_max
        
        # _pending is implemented as a list, rather than a deque, because
        # we do not need to popfirst. Instead, when it is full we simply
        # iterator over it and then re-initialize.
        self._pending = []
    
    def _flush_pending(self, work_done):
        logger.debug("Register:_flush_pending")
        tasks = self._pending
        self._pending = []
        try:
            self._client.add_subjects(tasks)
            for task in tasks:
                task.rtime = time.time()
                work_done(task)
        except Exception as e:
            work_done(e)
        

    def do_work(self, task, work_done):
        logger.debug('Register:do_work: %s' % task)
        
        if task is outbox.Outbox._REG_DONE:
            if len(self._pending) > 0:
                self._flush_pending(work_done)
            work_done(task)
            return
    
        assert isinstance(task, File)
        self._pending.append(task)
        if len(self._pending) >= self._bulk_ops_max:
            self._flush_pending(work_done)
