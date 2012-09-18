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
Management interface representing each Outbox.
"""

import logging
from tagfiler.iobox import worker, find, tag, register

logger = logging.getLogger(__name__)

# It is very likely that this DOES NOT need to be a Thread. It just needs to
# manage the pipeline of threads and/or threadpools that are doing the work.
# One argument for keeping this threaded would be that the outbox might need
# to be event-driven. But that's not certain right now.
class Outbox():
    """
    The class that represents each Outbox.
    
    It is responsible for initializing each outbox's threaded pipeline. It
    provides management interfaces to start and terminate the pipeline.
    """
    
    def __init__(self, outbox_model, state_dao):
        """Initializes the Outbox according to the required 'outbox_model'
        parameter of type 'models.Outbox'."""
        logger.debug("Outbox:__init__")
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
                               self._model.get_inclusion_patterns(),
                               self._model.get_exclusion_patterns())
        self._tag = tag.Tag(self._tag_q, self._register_q)
        self._register = register.Register(
                                    self._register_q, worker.WorkQueue(),
                                    self._state_dao, 
                                    self._model.get_tagfiler())
        

    def terminate(self):
        """Flags the outbox to terminate gracefully."""
        logger.debug("Outbox:terminate")
        self._terminated = True
        self._find.terminate()
        self._tag.terminate()
        self._register.terminate()
        
    def is_terminated(self):
        """Indicates whether the outbox (and its pipeline) have terminated cleanly."""
        return not (self._find.is_alive() or self._tag.is_alive() or self._register.is_alive())
        
    def start(self):
        """""Starts the outbox pipeline."""
        logger.debug("Outbox:start")
        self._register.start()
        self._tag.start()
        self._find.start()
        
    def join(self):
        """Q&D thread synchronized termination for now, but need to really do this later"""
        self._find_q.join()
        self._tag_q.join()
        self._register_q.join()
