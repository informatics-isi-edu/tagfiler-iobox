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
The main process for the Outbox.

TODO: For now a simple process, but it will be turned into a daemon using
the python standard daemon library.
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
    The Outbox class represents one outbox.
    
    It is the thread the continually scans the file system for new or modified
    files. It manages the scanning pipeline.
    """
    
    def __init__(self, outbox_model):
        """Initializes the Outbox.
        
        Arguments:
            'outbox_model': the models.Outbox object the represents the
                configuration of this outbox.
        """
        logger.debug("__init__")
        self._model = outbox_model
        self._terminated = False
        
        # I really wonder whether I need to keep refs to these queues. Hmmmm...
        self._find_q = worker.WorkQueue()
        self._tag_q = worker.WorkQueue()
        self._register_q = worker.WorkQueue()

        # The pipeline consists of the Find, Tag, and Register workers with their
        # associated WorkQueues.
        self._find = find.Find(self._find_q, self._tag_q)
        self._tag = tag.Tag(self._tag_q, self._register_q)
        self._register = register.Register(self._register_q, worker.WorkQueue())
        
        # Get outbox and populate Find's queue with the root directories.
        self._find_q.put("/tmp")

    def terminate(self):
        """Flags the outbox to terminate gracefully."""
        logger.debug("terminate")
        self._terminated = True
        self._find.terminate()
        self._tag.terminate()
        self._register.terminate()
        
    def start(self):
        """""Starts the outbox pipeline."""
        logger.debug("start")
        self._register.start()
        self._tag.start()
        self._find.start()
        
    def join(self):
        """Q&D thread synchronized termination for now, but need to really do this later"""
        self._find_q.join()
        self._tag_q.join()
        self._register_q.join()
