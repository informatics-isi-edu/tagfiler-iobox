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
import threading
from tagfiler.iobox import worker, find, tag, register

logger = logging.getLogger(__name__)


class Outbox(threading.Thread):
    """
    The Outbox class represents one outbox.
    
    It is the thread the continually scans the file system for new or modified
    files. It manages the scanning pipeline.
    """
    
    def __init__(self, outbox_config):
        """Initializes the Outbox.
        
        Arguments
            outbox_config: the OutboxConfiguration object.
        """
        logger.debug("__init__")
        threading.Thread.__init__(self)
        self.setDaemon(True)            #TODO: uncomment this when done testing
        self._config = outbox_config
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

    def terminate(self):
        """Flags the Outbox to exit gracefully."""
        logger.debug("terminate")
        self._terminated = True

    def run(self):
        """
        Continuously scans.
        """
        logger.debug("Outbox:run")

        # Start the pipeline
        self._register.start()
        self._tag.start()
        self._find.start()
        
        # Wait for pipeline to finish
        self._find_q.join()
        self._tag_q.join()
        self._register_q.join()

        logger.debug("Outbox:done")
        
    def join(self):
        """Q&D thread synchronized termination for now, but need to really do this later"""
        self._find_q.join()
        self._tag_q.join()
        self._register_q.join()
