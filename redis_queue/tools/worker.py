#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Description:
    Worker process: ties to the given queue and handle all incoming tasks.
    You may run as many workers as you wish.

History:
   * 2010-03-25T09:17:55+0300 Initial commit. Version 0.1 released.
"""

__author__ = 'Nikolay Panov (author@niksite.ru)'
__license__ = 'GPLv3'
__version__ = 0.1
__updated__ = '2010-10-24 12:14:08 nik'


import time
import sys
import optparse
import signal

from redis_queue import Queue


class Worker:

    def __init__(self, **kwargs):
        self.SHUTDOWN_IN_PROGRESS = False
        self.register_signal_handlers()
        self.run(**kwargs)

    def run(self, queue_name='default', sleep_time=1, verbose=False):
        """Worker process: ties to the given queue and handle all incoming tasks

        @queue_name - name of the queue to tie with
        @sleep_time - pause time between two tasks"""
        queue = Queue()
        while not self.SHUTDOWN_IN_PROGRESS:
            try:
                if verbose:
                    print '[remaining tasks number: %s] ' % queue.length(queue_name),
                result = queue.pop(queue_name=queue_name, timeout=10)
            except Exception, e:
                result = None
                print >> sys.stderr, 'An exception occurs during the proc processing: %s' % e
            if verbose:
                print result
            time.sleep(sleep_time)

    def register_signal_handlers(self):
        signal.signal(signal.SIGTERM, self.shutdown)
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGQUIT, self.shutdown)
        signal.signal(signal.SIGUSR1, self.shutdown)

    def shutdown(self, signum, frame):
        self.SHUTDOWN_IN_PROGRESS = True


if __name__ == "__main__":
    """Starts the workers main loop."""
    option_list = (
        optparse.make_option('-q', '--queue', default='default',
            action="store", dest="queue_name",
            help="Name of the queue to tie with."),
        optparse.make_option('-p', '--pause', default=1,
            action="store", dest="sleep_time", type="int",
            help="Pause time between two tasks."),
        optparse.make_option('-v', '--verbose', default=False,
            action="store_true", dest="verbose",
            help="Whether to print tasks result value."),
        )
    parser = optparse.OptionParser(option_list=option_list)
    options, values = parser.parse_args(sys.argv[1:])
    Worker(**vars(options))

# Emacs:
# Local variables:
# time-stamp-pattern: "100/^__updated__ = '%%'$"
# End:
