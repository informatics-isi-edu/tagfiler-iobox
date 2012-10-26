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

import worker, find, cksum, tag, register, models, dispatcher
from tagfiler.util import rules
import logging


logger = logging.getLogger(__name__)


class Outbox():
    """This class represents each Outbox and manages its operations."""
    
    def __init__(self, outbox_model):
        """Initializes the Outbox.
        
        The 'outbox_model' parameter is an instance of models.Outbox.
        """
        logger.debug("Outbox:__init__")
        
        assert isinstance(outbox_model, models.Outbox)
        
        self._model = outbox_model
        self._terminated = False
        
        self._find_q = worker.WorkQueue()
        self._sum_q = worker.WorkQueue()
        self._tag_q = worker.WorkQueue()
        self._register_q = worker.WorkQueue()
        self._dispatch_q = worker.WorkQueue()
        
        # Populate Find's queue with the root directories.
        for root in self._model.get_roots():
            self._find_q.put(root)
            logger.debug("Added root %s to the Find queue" % str(root))

        # The pipeline consists of the Find, Tag, and Register workers with their
        # associated WorkQueues.
        self._find = find.Find(self._find_q, self._dispatch_q, 
                               self._model.get_inclusion_patterns(),
                               self._model.get_exclusion_patterns())
        
        self._sum = cksum.Checksum(self._sum_q, self._dispatch_q)
        
        self._tag = tag.Tag(self._tag_q, self._register_q, 
                            self._model.get_all_rules(),
                            rules.TagDirector())
        
        self._register = register.Register(
                                    self._register_q, self._dispatch_q,
                                    self._model.get_tagfiler())
        
        self._dispatcher = dispatcher.Dispatcher(self._model.state_db,
                                                 self._dispatch_q, 
                                                 self._sum_q,
                                                 self._tag_q,
                                                 self._register_q)
        
        
    def start(self):
        """Starts the Outbox."""
        logger.debug("Outbox:start")
        
        assert self._terminated != True
        
        self._dispatcher.start()
        self._register.start()
        self._tag.start()
        self._sum.start()
        self._find.start()

    def terminate(self):
        """Flags the Outbox to terminate gracefully."""
        logger.debug("Outbox:terminate")
        
        assert self._terminated != True
        
        self._terminated = True
        self._find.terminate()
        self._dispatcher.terminate()
        self._sum.terminate()
        self._tag.terminate()
        self._register.terminate()
        
    def is_terminated(self):
        """Indicates whether the Outbox has terminated."""
        return not (self._find.is_alive() or 
                    self._sum.is_alive() or
                    self._tag.is_alive() or 
                    self._register.is_alive() or
                    self._dispatcher.is_alive())
        
    def join(self):
        """QuickNDirty thread synchronization. TODO: need to re-do this later."""
        self._find_q.join()
        self._dispatch_q.join()
        self._sum_q.join()
        self._dispatch_q.join()
        self._tag_q.join()
        self._dispatch_q.join()
        self._register_q.join()
        self._dispatch_q.join()
