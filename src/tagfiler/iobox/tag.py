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
from tagfiler.util import rules

logger = logging.getLogger(__name__)


class Tag(worker.Worker):
    """A worker for performing the tagging stage of the outbox pipeline."""
    
    def __init__(self, tasks, results, state_dao, all_rules, tag_director):
        super(Tag, self).__init__(tasks, results)
        
        assert isinstance(state_dao, dao.OutboxStateDAO)
        assert isinstance(tag_director, rules.TagDirector)
        self._state_dao = state_dao
        self._rules = all_rules or []
        self._tag_director = tag_director

    def do_work(self, task, work_done):
        assert isinstance(task, models.File)
        fileobj = task
        logger.debug('do_work: File: %s' % fileobj)
        reg_file = self._state_dao.register_file(fileobj) 

        self._tag_director.tag_registered_file(self._rules, reg_file)
        
        for tag in reg_file.get_tags():
            self._state_dao.add_registered_file_tag(reg_file, tag)
        reg_file.get_file().set_must_tag(False)
        self._state_dao.update_file(reg_file.get_file())
        work_done(reg_file)
