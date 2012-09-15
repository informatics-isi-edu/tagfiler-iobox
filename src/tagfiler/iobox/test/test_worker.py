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
Unit tests for the worker module.
"""

import unittest
import logging
import time
import tagfiler.iobox.worker as worker

logger = logging.getLogger(__name__)

class DummyWorker(worker.Worker):
    """A test worker.
    
    The test worker simulates a stage in a worker thread pipeline. It takes a
    task queue for inputs to the stage and a resutls queue as the output for
    the stage. It takes a worker_id to add to print in the log. And it
    generates a (results_per_task) number of dummy results for every task it
    takes off the intput tasks queue.
    """
    
    def __init__(self, tasks, results, worker_id, results_per_task):
        """Initializes the Worker class.
        
        Arguments:
        tasks -- a WorkQueue of tasks.
        results -- a WorkQueue of results.
        worker_id -- an identifier for this test worker.
        generate_num_tasks -- the number of results per task to produce. 
        """
        worker.Worker.__init__(self, tasks, results)
        self.results_per_task = results_per_task
        self.worker_id = worker_id
    
    def do_work(self, task, work_done):
        logger.debug('Stage(%d) Task: %d' % (self.worker_id, task))
        for i in range(self.results_per_task):
            result = i
            work_done(result)


class WorkerTest(unittest.TestCase):
    """The Unit Tests for worker module."""

    def testSingleStagePipeline(self):
        """Tests a single stage (i.e., single worker) pipeline."""
        
        # Create task queues for the pipeline.
        task_queue_1 = worker.WorkQueue()
        task_queue_2 = worker.WorkQueue()
    
        # Populate the first queue with tasks, the other queues will get
        # filled by the the pipelined workers.
        for i in range(10):
            task_queue_1.put(i)
        #task_queue_1.put(worker.Worker.DONE)
    
        # Create the test workers
        stage1 = DummyWorker(task_queue_1, task_queue_2, 1, 10)
    
        # Start them in reverse order so they are waiting for stage 1 to start
        # producing tasks to propagate throught the pipeline.
        stage1.start()
    
        # Now wait/join on the task queues so that the main thread will not exit
        # before the workers have finished.
        task_queue_1.join()
        stage1.terminate()
        
        time.sleep(1) #TODO(schuler): hate to do it this way
        self.assertFalse(stage1.is_alive(), "Worker is alive")
        
    def testThreeStagePipeline(self):
        """Tests a three stage pipeline."""
        
        # Create task queues for the pipeline.
        task_queue_1 = worker.WorkQueue()
        task_queue_2 = worker.WorkQueue()
        task_queue_3 = worker.WorkQueue()
    
        # Populate the first queue with tasks, the other queues will get
        # filled by the the pipelined workers.
        for i in range(10):
            task_queue_1.put(i)
        #task_queue_1.put(worker.Worker.DONE)
    
        # Create the test workers
        stage1 = DummyWorker(task_queue_1, task_queue_2, 1, 10)
        stage2 = DummyWorker(task_queue_2, task_queue_3, 2, 5)
        stage3 = DummyWorker(task_queue_3, worker.WorkQueue(), 3, 5)
    
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
        stage1.terminate()
        stage2.terminate()
        stage3.terminate()
        
        time.sleep(1) #TODO(schuler): hate to do it this way
        self.assertFalse(stage1.is_alive(), "Stage 1 worker is alive")
        self.assertFalse(stage2.is_alive(), "Stage 2 worker is alive")
        self.assertFalse(stage3.is_alive(), "Stage 3 worker is alive")


if __name__ == "__main__":
    """A standaline test of the three stage pipeline."""
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
