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
Implements the checksum stage of the Outbox.
"""

from worker import Worker
from models import File
from tagfiler.util import files
import outbox

import logging


logger = logging.getLogger(__name__)


class Checksum(Worker):
    """The checksum pipeline worker."""

    def do_work(self, task, work_done):
        logger.debug('Checksum:do_work: %s' % task)
        
        if task is outbox.Outbox._SUM_DONE:
            work_done(task)
            return
        
        try:
            assert isinstance(task, File)
            checksum = files.sha256sum(task.filename) #TODO: this needs to be interuptable
            if task.status == File.COMPUTE:
                task.checksum = checksum
            else:
                task.compare = checksum
            work_done(task)
        except Exception as e:
            work_done(e)
