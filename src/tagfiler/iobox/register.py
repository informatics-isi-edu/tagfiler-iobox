'''
Placeholder for the register module.
'''

import logging
import worker

logger = logging.getLogger(__name__)

class Register(worker.Worker):
    
    def do_work(self, task, work_done):
        logger.debug('Task:        %d' % task)
        return
