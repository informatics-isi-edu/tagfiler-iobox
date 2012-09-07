'''
Placeholder for the tag module.
'''

import logging
import worker

logger = logging.getLogger(__name__)

class Tag(worker.Worker):
    '''A worker for performing the tag stage of the outbox pipeline.'''
    
    def do_work(self, task, work_done):
        logger.debug('Task:    %d' % task)
        work_done(task)
        return