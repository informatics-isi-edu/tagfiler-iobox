'''
Placeholder for a proper Python unittest module.
'''

import logging
import tagfiler.iobox.worker as worker

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TestWorker(worker.Worker):
    '''A test worker.
    
    The test worker simulates a stage in a worker thread pipeline. It takes a
    task queue for inputs to the stage and a resutls queue as the output for
    the stage. It takes a worker_id to add to print in the log. And it
    generates a (results_per_task) number of dummy results for every task it
    takes off the intput tasks queue.
    '''
    
    def __init__(self, tasks, results, worker_id, results_per_task):
        '''Initializes the Worker class.
        
        Arguments:
        tasks -- a WorkQueue of tasks.
        results -- a WorkQueue of results.
        worker_id -- an identifier for this test worker.
        generate_num_tasks -- the number of results per task to produce. 
        '''
        worker.Worker.__init__(self, tasks, results)
        self.results_per_task = results_per_task
        self.worker_id = worker_id
    
    def do_work(self, task, work_done):
        logger.debug('Stage(%d) Task: %d' % (self.worker_id, task))
        for i in range(self.results_per_task):
            result = i
            work_done(result)
        return


if __name__ == '__main__':
    
    logger.info('TEST BEGIN')
    
    # Create task queues for the pipeline.
    task_queue_1 = worker.WorkQueue()
    task_queue_2 = worker.WorkQueue()
    task_queue_3 = worker.WorkQueue()

    # Populate the first queue with tasks, the other queues will get
    # filled by the the pipelined workers.
    for i in range(10):
        task_queue_1.put(i)
    task_queue_1.put(worker.Worker.DONE)

    # Create the test workers
    stage1 = TestWorker(task_queue_1, task_queue_2, 1, 10)
    stage2 = TestWorker(task_queue_2, task_queue_3, 2, 5)
    stage3 = TestWorker(task_queue_3, worker.WorkQueue(), 3, 5)

    # Start them in reverse order so they are waiting for stage 1 to start
    # producing tasks to propagate throught the pipeline.
    stage3.start()
    stage2.start()
    stage1.start()

    # Now wait/join on the task queues so that the main thread will not exit
    # before the workers have finished.
    task_queue_1.join()
    task_queue_2.join()
    task_queue_3.join()

    logger.info('TEST END')
