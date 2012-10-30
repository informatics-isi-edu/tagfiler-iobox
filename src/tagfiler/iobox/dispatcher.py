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
This module implements the 'Dispatcher' of the Tagfiler Outbox pipeline. It 
works on an input queue that comes from the pipeline workers. It may perform
persistent checkpointing of the state of the pipeline. It then dispatches work
to the next workers task queue.
"""

from tagfiler.iobox.worker import Worker
from tagfiler.iobox.dao import OutboxStateDAO
from tagfiler.iobox.models import File

import logging


logger = logging.getLogger(__name__)


class Dispatcher(Worker):
    """The worker thread for the 'Dispatcher' for the Tagfiler Outbox."""
    
    def __init__(self, state_db, tasks, sumq, tagq, registerq):
        """Initializes the object."""
        super(Dispatcher, self).__init__(tasks, None)
        self._state = None
        self._state_db = state_db
        self._sumq = sumq
        self._tagq = tagq
        self._regq = registerq
        
    def on_start(self):
        """Initializes the Outbox state persistence object."""
        self._state = OutboxStateDAO(self._state_db)
    
    def on_terminate(self, work_done):
        """Closes the outbox state persistence object."""
        self._state.close()

    def do_work(self, task, work_done):
        
        if task.status is None:
            exists = self._state.find_file(task.filename)
            if not exists:
                logger.debug("New: %s" % task.filename)
                task.status = File.COMPUTE
                self._sumq.put(task)
            elif task.mtime > exists.mtime:
                logger.debug("Modified: %s" % task.filename)
                task.id = exists.id
                task.checksum = exists.checksum
                task.status = File.COMPARE
                self._sumq.put(task)
            elif not exists.rtime:
                logger.debug("Not registered: %s" % task.filename)
                task.status = File.REGISTER
                self._tagq.put(task)
            else:
                logger.debug("Skipping: %s" % task.filename)
        
        elif task.status == File.COMPUTE:
            task.status = File.REGISTER
            self._state.add_file(task)
            self._tagq.put(task)
            
        elif task.status == File.COMPARE:
            if task.checksum != task.compare:
                task.status = File.REGISTER
                task.checksum = task.compare
                self._state.update_file(task)
                self._tagq.put(task)
            elif not exists.rtime:
                logger.debug("Not registered: %s" % task.filename)
                task.status = File.REGISTER
                self._tagq.put(task)
            else:
                logger.debug("Unchanged: %s" % task.filename)
                # Update its mtime so that it won't be cksummed next time
                self._state.update_file(task) #TODO: this should be update_file_mtime(...)
            
        elif task.status == File.REGISTER:
            self._state.update_file(task)
