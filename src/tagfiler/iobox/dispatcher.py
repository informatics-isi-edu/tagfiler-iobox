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
import outbox

import logging


logger = logging.getLogger(__name__)


class Dispatcher(Worker):
    """The worker thread for the 'Dispatcher' for the Tagfiler Outbox."""
    
    def __init__(self, donecb, a, state_db, tasks, sumq, tagq, registerq):
        """Initializes the object."""
        super(Dispatcher, self).__init__(tasks, None)
        self._donecb = donecb
        self._a = a
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
        logger.debug("do_work: %s" % task)
        
        #
        # Process control flow flags, first
        #
        if task is outbox.Outbox._FIND_DONE:
            self._sumq.put(outbox.Outbox._SUM_DONE)
            return
        elif task is outbox.Outbox._SUM_DONE:
            self._tagq.put(outbox.Outbox._TAG_DONE)
            return
        elif task is outbox.Outbox._TAG_DONE:
            self._regq.put(outbox.Outbox._REG_DONE)
            return
        elif task is outbox.Outbox._REG_DONE:
            self._donecb(self._a)
            return
        
        #
        # Process persistent checkpointing
        #
        if task.status is None:
            # Case: we are in the FIND stage
            exists = self._state.find_file(task.filename)
            if exists: task.id = exists.id
                
            if not exists:
                # Case: New file, not seen before
                logger.debug("New: %s" % task.filename)
                task.status = File.COMPUTE
                self._sumq.put(task)
            elif task.mtime > exists.mtime:
                # Case: File has changed since last seen
                logger.debug("Modified: %s" % task.filename)
                task.checksum = exists.checksum
                task.rtime = exists.rtime
                task.status = File.COMPARE
                self._sumq.put(task)
            elif task.size and not exists.checksum:
                # Case: Missing checksum, on regular file
                logger.debug("Missing checksum: %s" % task.filename)
                task.checksum = exists.checksum
                task.rtime = exists.rtime
                task.status = File.COMPARE
                self._sumq.put(task)
            elif not exists.rtime:
                # Case: File has not been registered
                logger.debug("Not registered: %s" % task.filename)
                task.status = File.REGISTER
                self._tagq.put(task)
            else:
                # Case: File does not meet any criteria for processing
                logger.debug("Skipping: %s" % task.filename)
        
        elif task.status == File.COMPUTE:
            # Case: we are in the post Checksum COMPUTE stage
            task.status = File.REGISTER
            self._state.add_file(task)
            self._tagq.put(task)
            
        elif task.status == File.COMPARE:
            # Case: we are in the post Checksum COMPARE stage
            if task.checksum != task.compare:
                # Case: checksums differ, need to re-tag and register
                task.status = File.REGISTER
                task.checksum = task.compare
                self._state.update_file(task)
                self._tagq.put(task)
            elif not task.rtime:
                # Case: File has not been registered
                logger.debug("Not registered: %s" % task.filename)
                task.status = File.REGISTER
                self._tagq.put(task)
            else:
                # Case: File checksum matches, update mtime only
                logger.debug("Unchanged: %s" % task.filename)
                # Update its mtime so that it won't be cksummed next time
                self._state.update_file(task)
            
        elif task.status == File.REGISTER:
            # Case: we are in the post REGISTER stage
            logger.debug("Update file: %s" % task.filename)
            self._state.update_file(task)
