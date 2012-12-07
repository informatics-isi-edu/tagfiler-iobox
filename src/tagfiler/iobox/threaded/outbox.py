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
Outbox management.
"""

import worker, find, cksum, tag, register, dispatcher
from tagfiler.iobox import models
from tagfiler.util import rules

import logging
import threading


logger = logging.getLogger(__name__)


class Outbox():
    """This class represents each Outbox and manages its operations."""
    
    # Control flow flags
    _FIND_DONE  =   'FIND_DONE'
    _SUM_DONE   =   'SUM_DONE'
    _TAG_DONE   =   'TAG_DONE'
    _REG_DONE   =   'REG_DONE'
    
    def __init__(self, outbox_model, client):
        """Initializes the Outbox.
        
        The 'outbox_model' parameter is an instance of models.Outbox.
        """
        logger.debug("Outbox:__init__")
        
        assert isinstance(outbox_model, models.Outbox)
        
        self._model = outbox_model
        self._terminated = False
        self._done = False
        self._cv_done = threading.Condition()
        self._lock_terminate = threading.Lock()
        self.errors = []
        self.found = 0
        self.skipped = 0
        self.registered = 0
        
        self._find_q = worker.WorkQueue()
        self._sum_q = worker.WorkQueue()
        self._tag_q = worker.WorkQueue()
        self._register_q = worker.WorkQueue()
        self._dispatch_q = worker.WorkQueue()
        
        # Populate Find's queue with the root directories.
        for root in self._model.roots:
            self._find_q.put(root)

        # The pipeline consists of the Find, Tag, and Register workers with their
        # associated WorkQueues.
        self._find = find.Find(self._find_q, self._dispatch_q, 
                               excludes=self._model.excludes,
                               includes=self._model.includes)
        
        self._sum = cksum.Checksum(self._sum_q, self._dispatch_q)
        
        self._tag = tag.Tag(self._tag_q, self._register_q, 
                            self._model.path_rules,
                            rules.TagDirector())
        
        self._register = register.Register(
                                    self._register_q, self._dispatch_q,
                                    client, self._model.bulk_ops_max)
        
        self._dispatcher = dispatcher.Dispatcher(self._model.state_db,
                                                 self._dispatch_q, 
                                                 self._sum_q,
                                                 self._tag_q,
                                                 self._register_q,
                                                 self._dispatcher_done)
        
        
    def start(self):
        """Starts the Outbox."""
        logger.debug("Outbox:start")
        self._lock_terminate.acquire()
        assert self._terminated != True
        self._dispatcher.start()
        self._register.start()
        self._tag.start()
        self._sum.start()
        self._find.start()
        self._lock_terminate.release()

    def terminate(self):
        """Terminate the Outbox immediately.
        
        The Outbox will flag all threads to terminate immediately. The workers
        will not attempt to finish pending tasks. They will cleanup safely, for
        instance, closing and releasing any external resources.
        """
        logger.debug("Outbox:terminate")
        self._lock_terminate.acquire()
        assert self._terminated != True
        self._find.terminate()
        self._sum.terminate()
        self._tag.terminate()
        self._register.terminate()
        self._dispatcher.terminate()
        self._terminated = True
        self._lock_terminate.release()
        
    def is_terminated(self):
        """Has the Outbox been told to terminate?"""
        self._lock_terminate.acquire()
        terminated = self._terminated
        self._lock_terminate.release()
        return terminated
        
    def is_alive(self):        
        """Has the Outbox actually terminated?
        
        This indicates whether any thread in the Outbox pipeline is still 
        alive.
        """
        return not (self._find.is_alive() or 
                    self._sum.is_alive() or
                    self._tag.is_alive() or 
                    self._register.is_alive() or
                    self._dispatcher.is_alive())
        
    def done(self):
        """Done with the Outbox.
        
        The Outbox will process pending tasks. When the pending tasks are
        completed, the Outbox will set the done flag which may be checked
        by calling 'is_done'.
        
        Note that the Outbox will not terminate. When it is done, you should
        call 'terminate()' before exiting so that the Outbox workers will 
        release any external resources.
        """
        self._find_q.put(Outbox._FIND_DONE)
        
    def is_done(self):
        """Is the Outbox done?"""
        return self._done
        
    def _dispatcher_done(self, a):
        """Callback for the dispatcher worker, on completion of the pipeline.
        
        The 'a' parameter is an unused argument required by the callback.
        """
        logger.debug("_dispatcher_done")
        self._cv_done.acquire()
        self._done = True
        self.errors = self._dispatcher.errors
        self.found = self._dispatcher.found
        self.skipped = self._dispatcher.skipped
        self.registered = self._dispatcher.registered
        self._cv_done.notify_all()
        self._cv_done.release()
        
    def wait_done(self, timeout=None):
        """Wait for Outbox to complete its tasks.
        
        If 'timeout' is None, this call will block until the Outbox is done.
        """
        self._cv_done.acquire()
        if not self._done:
            self._cv_done.wait(timeout)
        self._cv_done.release()
