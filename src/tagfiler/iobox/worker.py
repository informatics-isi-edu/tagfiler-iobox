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
A simple worker thread module, intended for use in a threaded pipeline.
"""

import logging
import threading
import Queue

logger = logging.getLogger(__name__)


class WorkQueue(Queue.Queue):
    """A simple work queue.
    
    Right now it does nothing more than the underlying Queue.Queue class. This
    implementatoin should be replaced with a composition of rather than a
    subclass of the Queue.Queue.
    """
    
    # We probably want to hide the Queue implementation by wrapping a Queue 
    # instance in a WorkQueue so that we can control the methods exposed to
    # the caller. For now, we pass...
    pass

class Worker(threading.Thread):
    """A simple worker thread.
    
    The worker thread takes a task queue and a results queue as parameters to
    its initializer. For each task, it may produce 0-N results which are
    put on the results queue. If no task is available on the task queue, it
    blocks for the next available task. When it processed the DONE marker, it
    terminates.
    """

    # Internal marker added to the input queue to unblock a waiting worker.
    __TERMINATE = '__TERMINATE'
        
    def __init__(self, tasks, results):
        """Initializes the Worker class.
        
        Arguments:
            tasks: a WorkQueue of tasks.
            results: a WorkQueue of results.
        """
        super(Worker, self).__init__()
        assert tasks is not None
        assert hasattr(tasks, 'get')
        if results is not None:
            assert hasattr(results, 'put')
            assert hasattr(results, 'put_nowait')
            pass
        
        self.setDaemon(True)
        self._terminate = False
        self._tasks = tasks
        self._results = results
    
    def terminate(self):
        """Flags the worker to terminate cleanly."""
        assert not self._terminate
        self._terminate = True
        try:
            # Need to put a dummy task on the queue in case the worker is
            # blocking on input.
            self._tasks.put_nowait(Worker.__TERMINATE)
        except Queue.Full:
            # The thread should be okay if the queue is full. That should
            # mean that the thread will not be blocking on the queue, and
            # the thread will still terminate without the __TERMINATE marker 
            # because it is really looking for the _terminate flag.
            pass
    
    def on_start(self):
        """Called during the start of operations.
        
        This method may be overriden by subclasses that want to perform some 
        one-time initialization before work begins.
        """
        pass
    
    def do_work(self, task, work_done):
        """Called whenever a new task is available.
        
        This method should be overridden by subclasses. When the task is done 
        call the work_done function, giving it 1 argument, the result object 
        to be passed back from this worker.
        
        Note that is the Worker subclass performs a long running operation 
        during the call to do_work, it should check on the terminate flag 
        frequently. If terminate is True, the do_work should immediately clean 
        up and terminate.
        """
        pass
    
    def on_terminate(self):
        """Called during termination.
        
        This method may be overriden by subclasses that want to perform some
        one-time cleanup before the thread exits.
        """
        pass
    
    # Might want to turn do_work into a generator function and instead of using
    # this _work_done callback, the do_work just yields results whenever.
    # Also, _work_done is misleading because the task may not be 'done' instead
    # it is really just returning (or yielding) the next result.
    def _work_done(self, result):
        """
        Internal implementation of the work_done callback passed to the
        do_work function.
        """
        self._results.put(result)
        return
        
    def run(self):
        """Subclasses of Worker should not override this method."""
        
        logger.debug('run:BEGIN')
        self.on_start()

        while not self._terminate:
            task = self._tasks.get()
            if task == Worker.__TERMINATE:
                break
            self.do_work(task, self._work_done)
            self._tasks.task_done()

        self.on_terminate()
        logger.debug('run:END')
