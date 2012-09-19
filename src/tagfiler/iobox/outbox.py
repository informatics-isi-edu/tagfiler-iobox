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
Implements Outbox management.
"""

import worker, find, tag, register, dao, models

import logging


logger = logging.getLogger(__name__)


class Outbox():
    """This class represents each Outbox and manages its operations."""
    
    def __init__(self, outbox_model, state_dao):
        """Initializes the Outbox.
        
        The 'outbox_model' parameter is an instance of models.Outbox.
        
        The 'state_dao' parameter is an instance of dao.OutboxStateDAO.
        """
        logger.debug("Outbox:__init__")
        
        assert isinstance(outbox_model, models.Outbox)
        assert isinstance(state_dao, dao.OutboxStateDAO)
        
        self._model = outbox_model
        self._state_dao = state_dao
        self._terminated = False
        
        self._find_q = worker.WorkQueue()
        self._tag_q = worker.WorkQueue()
        self._register_q = worker.WorkQueue()
        
        # Populate Find's queue with the root directories.
        for root in self._model.get_roots():
            self._find_q.put(root)

        # The pipeline consists of the Find, Tag, and Register workers with their
        # associated WorkQueues.
        self._find = find.Find(self._find_q, self._tag_q, 
                               self._state_dao,
                               self._model.get_inclusion_patterns(),
                               self._model.get_exclusion_patterns())
        self._tag = tag.Tag(self._tag_q, self._register_q, 
                            self._state_dao,
                            self._model.get_path_rules()) #TODO(schuler): TBD to verify whether this is the correct parameter for Tag init
        self._register = register.Register(
                                    self._register_q, worker.WorkQueue(),
                                    self._model.get_tagfiler())
        
        
    def start(self):
        """""Starts the Outbox."""
        logger.debug("Outbox:start")
        
        assert self._terminated != True
        
        self._register.start()
        self._tag.start()
        self._find.start()

    def terminate(self):
        """Flags the Outbox to terminate gracefully."""
        logger.debug("Outbox:terminate")
        
        assert self._terminated != True
        
        self._terminated = True
        self._find.terminate()
        self._tag.terminate()
        self._register.terminate()
        
    def is_terminated(self):
        """Indicates whether the Outbox has terminated."""
        return not (self._find.is_alive() or 
                    self._tag.is_alive() or 
                    self._register.is_alive())
        
    def join(self):
        """QuickNDirty thread synchronization. TODO: need to re-do this later."""
        self._find_q.join()
        self._tag_q.join()
        self._register_q.join()
