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

import worker, dao, models
from tagfiler.util.http import TagfilerClient

import logging


logger = logging.getLogger(__name__)


class Register(worker.Worker):
    """The registration pipeline worker."""
    
    def __init__(self, tasks, results, state_dao, tagfiler):
        """Constructor.
        
        Arguments:
            tasks: A queue of models.RegisterFile instances.
            results: An empty queue.
            state_dao: the OutboxStateDAO instance.
            tagfiler: the models.Tagfiler instance.
        """
        super(Register, self).__init__(tasks, results)
        
        assert isinstance(state_dao, dao.OutboxStateDAO)
        assert isinstance(tagfiler, models.Tagfiler)
        
        self._state_dao = state_dao
        self._client = TagfilerClient(tagfiler)

    def do_work(self, task, work_done):
        """Performs register work on a task.
        
        Arguments:
            task: register file object to add to tagfiler
            work_done: callback to run on the registered file.
        """
        assert isinstance(task, models.RegisterFile)
        reg_file = task
        logger.debug('Register:do_work: %s' % reg_file)
        self._client.add_subject(task)
        
        # TODO: cleanup register_file entry in the database.  SQLite won't allow an object constructed in one
        # thread to be used in another -- do I have to create a DAO in each do_work invocation?
        self._state_dao.remove_registered_file_and_tags(reg_file)
        work_done(reg_file) #TODO(schuler): or emit None, since register is the end of the pipeline
