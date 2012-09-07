'''
Placeholder for a proper Python unittest module.
'''

import logging
from tagfiler.iobox.worker import WorkQueue, Worker
from tagfiler.iobox import find, tag, register

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    
    logger.info('TEST BEGIN')
    
    # Create Find worker tasks
    find_tasks = WorkQueue()
    for i in range(3):
        find_tasks.put(i)
    find_tasks.put(Worker.DONE)

    # Create Tag worker tasks    
    tag_tasks = WorkQueue()
    
    # Create Register worker tasks
    register_tasks = WorkQueue()
    
    final = WorkQueue()
    
    f = find.Find(find_tasks, tag_tasks)
    t = tag.Tag(tag_tasks, register_tasks)
    r = register.Register(register_tasks, final)
    r.start()
    t.start()
    f.start()

    # The following join calls are not working as expected... TBD!
    #find_tasks.join()
    #tag_tasks.join()
    #register_tasks.join()
    #final.join()

    logger.info('TEST END')
