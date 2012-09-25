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
Implements the tagging stage of the Outbox pipeline.
"""

import logging
import worker, dao, models


logger = logging.getLogger(__name__)


class Tag(worker.Worker):
    """A worker for performing the tagging stage of the outbox pipeline."""
    
    def __init__(self, tasks, results, state_dao, rules, rules_director):
        super(Tag, self).__init__(tasks, results)
        
        assert isinstance(state_dao, dao.OutboxStateDAO)
        
        self._state_dao = state_dao
        self._rules = rules
        self._rules_director = rules_director

    def do_work(self, task, work_done):
        assert isinstance(task, models.File)
        fileobj = task
        logger.debug('do_work: File: %s' % fileobj)
        reg_file = self._state_dao.register_file(fileobj)
        self._rules_director.tag_registered_file(self._rules, reg_file)
        # TODO: fix multi-threading in SQLite
        for tag in reg_file.get_tags():
            self._state_dao.add_registered_file_tag(reg_file, tag)
        work_done(reg_file)
