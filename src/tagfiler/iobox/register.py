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
Placeholder for the register module.
"""

import logging
import worker
from tagfiler.util.http import TagfilerClient
from tagfiler.iobox.dao import OutboxDAO
import os

logger = logging.getLogger(__name__)

class Register(worker.Worker):
    
    def __init__(self, tasks, results, config):
        """Constructor
        
        Keyword arguments:
        tasks -- ?
        results -- ?
        dao -- the outbox state dao that this task is associated with
        config -- the tagfiler configuration to use to communicate with the service
        """
        super(Register, self).__init__(tasks, results)
        self._config = config
        self._client = TagfilerClient(config)

    def do_work(self, task, work_done):
        """Performs register work on a task.
        
        Keyword arguments:
        task -- register file object to add to tagfiler
        work_done -- callback to run on the registered file.
        """
        logger.debug('Task:        %s' % task)
        self._client.add_subject(task)
        
        # TODO: cleanup register_file entry in the database.  SQLite won't allow an object constructed in one
        # thread to be used in another -- do I have to create a DAO in each do_work invocation?
        
        work_done(task)
