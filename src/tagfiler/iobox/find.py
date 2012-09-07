'''
Placeholder for the find module.
'''

import logging
import worker

logger = logging.getLogger(__name__)

class Find(worker.Worker):
    '''A worker for performing the find stage of the outbox pipeline.'''
    
    def do_work(self, task, work_done):
        logger.debug('Find: %d' % task)
        for i in range(task, task+10):
            result = i
            work_done(result)
        return