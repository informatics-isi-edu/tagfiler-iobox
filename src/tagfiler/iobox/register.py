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
from models import Tagfiler
from models import File
from tagfiler.util.http import TagfilerClient

import time
import logging


logger = logging.getLogger(__name__)


class Register(Worker):
    """The registration pipeline worker."""
    
    def __init__(self, tasks, results, tagfiler):
        super(Register, self).__init__(tasks, results)
        assert isinstance(tagfiler, Tagfiler)
        self._client = TagfilerClient(tagfiler)

    def do_work(self, task, work_done):
        logger.debug('Register:do_work: %s' % task)
        assert isinstance(task, File)
        self._client.add_subject(task)
        task.rtime = time.time()
        task.status = File.REGISTERED
        work_done(task)
