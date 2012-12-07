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

import worker
from tagfiler.iobox import models
from tagfiler.util import rules
import outbox

import logging


logger = logging.getLogger(__name__)


class Tag(worker.Worker):
    """A worker for performing the tagging stage of the outbox pipeline."""
    
    def __init__(self, tasks, results, all_rules, tag_director):
        super(Tag, self).__init__(tasks, results)
        assert isinstance(tag_director, rules.TagDirector)
        self._rules = all_rules or []
        self._tag_director = tag_director

    def do_work(self, task, work_done):
        logger.debug('Tag:do_work: %s' % task)
        
        if task is outbox.Outbox._TAG_DONE:
            work_done(outbox.Outbox._REG_DONE)
            return
        
        try:
            assert isinstance(task, models.File)
            self._tag_director.tag_registered_file(self._rules, task)
            work_done(task)
        except Exception as e:
            work_done(e)
