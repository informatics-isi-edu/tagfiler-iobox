'''
This module implements a simple worker thread.

It is not yet complete or entirely correct. The idea is to implement a 
threaded pipeline.
'''

import logging
import threading
import Queue

logger = logging.getLogger(__name__)

class WorkQueue(Queue.Queue):
    '''A simple work queue implementation.'''
    
    # We probably want to hid the Queue implementation by wrapping a Queue 
    # instance in a WorkQueue so that we can control the methods exposed to
    # the caller. For now, we pass...
    pass

class Worker(threading.Thread):
    '''Generic worker thread class for IO Box operations.'''
    
    DONE = 'tagfiler.iobox.worker.Worker.DONE'
        
    def __init__(self, tasks, results):
        '''Initializes the Worker class.
        
        Arguments:
        tasks -- a WorkQueue of tasks.
        results -- a WorkQueue of results.
        '''
        threading.Thread.__init__(self)
        self.tasks = tasks
        self.results = results
    
    def do_work(self, task, work_done):
        '''Called whenever a new task is available.
        
        This is the only method that should be overridden by subclasses. When 
        the task is done call the work_done function, giving it 1 argument, the
        result object to be passed back from this worker.        
        '''
        pass
    
    def _work_done(self, result):
        '''
        Internal implementation of the work_done callback passed to the
        do_work function.
        '''
        self.results.put(result)
        return
        
    def run(self):
        '''Subclasses of Worker should not override this method.'''
        
        logger.debug('run:BEGIN')

        task = self.tasks.get()
        while task != Worker.DONE:
            self.do_work(task, self._work_done)
            self.tasks.task_done()
            task = self.tasks.get()

        self.results.put(Worker.DONE)

        logger.debug('run:END')
        return
