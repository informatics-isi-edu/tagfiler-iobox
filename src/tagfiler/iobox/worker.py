'''
A simple worker thread module, intended for use in a threaded pipeline.
'''

import logging
import threading
import Queue

logger = logging.getLogger(__name__)

class WorkQueue(Queue.Queue):
    '''A simple work queue.
    
    Right now it does nothing more than the underlying Queue.Queue class. This
    implementatoin should be replaced with a composition of rather than a
    subclass of the Queue.Queue.
    '''
    
    # We probably want to hide the Queue implementation by wrapping a Queue 
    # instance in a WorkQueue so that we can control the methods exposed to
    # the caller. For now, we pass...
    pass

class Worker(threading.Thread):
    '''A simple worker thread.
    
    The worker thread takes a task queue and a results queue as parameters to
    its initializer. The worker processes the tasks one-by-one until it reaches
    the DONE marker task. For each task, it may produce 0-N results which are
    put on the results queue. If no task is available on the task queue, it
    blocks for the next available task. When it processed the DONE marker, it
    terminates.
    
    We ought to add a terminate() call and reimplement the blocking behavior so
    that the thread can be terminated out of sequence.
    '''

    # The marker for the end of the work queue. Maybe this should be moved over
    # to the WorkQueue class.
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
        self.tasks.task_done()
        while task != Worker.DONE:
            self.do_work(task, self._work_done)
            task = self.tasks.get()
            self.tasks.task_done()

        self.results.put(Worker.DONE)

        logger.debug('run:END')
        return
