"""
The command-line interface to the tagfiler-outbox service.
"""

import logging
import tagfiler.iobox.outbox as outbox

logger = logging.getLogger(__name__)

# Exit return codes
__EXIT_SUCCESS = 0
__EXIT_FAILURE = 1

def main():
    """
    The main routine.
    """
    
    logging.basicConfig(level=logging.DEBUG)
    
    outbox_worker = outbox.Outbox(None)
    outbox_worker.start()
    # this doesn't exactly work and it is NOT how I want this to work, just a
    # little Q&D for now
    outbox_worker.join()
    
    print "main() done"
    
    return __EXIT_SUCCESS

