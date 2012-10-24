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

from tagfiler.iobox import worker
import logging


logger = logging.getLogger(__name__)


class Dispatcher(worker.Worker):
    """The worker thread for the 'Dispatcher' for the Tagfiler Outbox."""
    
    def __init__(self, tasks, tagq, registerq):
        """Initializes the object."""
        super(Dispatcher, self).__init__(tasks, None)
        self._tagq = tagq
        self._regq = registerq

        # Create or load state database
        #outbox_state_filepath = os.path.join(os.path.dirname(args.filename), "outbox_state.db")
        #state_dao = dao.OutboxStateDAO(None, outbox_state_filepath)

    def __del__(self):
        #statedao.close()
        pass

    def do_work(self, task, work_done):
        self._tagq.put(task)
