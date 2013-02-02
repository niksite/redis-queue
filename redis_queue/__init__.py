#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Description:
This module implementing a simple tasks queue based on Redis database.

History:
   * 2010-10-12 Version 0.2 released.
   * 2010-04-01 Initial commit to http://code.google.com/p/redis-queue/
   * 2010-03-22 Version 0.1 released.
"""

__author__ = 'Nikolay Panov (author@niksite.ru)'
__license__ = 'GPLv3'
__version__ = 0.2
__updated__ = '2010-10-24 12:13:19 nik'


import redis
import pickle
import time
import datetime


class Queue:
    """Usage example:
    >>> from redis_queue.queue import *
    >>> q = Queue()
    >>> q.push(fetch_html, url='niksite.ru', use_proxy=True)
    >>> q.pop()
    <html><title>...
    """

    def __init__(self, default_queue='default'):
        self.QUEUE_PREFIX = '0GRmGpDerPRTmV4h:'
        self.DB = redis.Redis(db=2)
        self.default_queue = default_queue

    def length(self, queue_name=None):
        """Return number of remaining tasks in the given queue
        """
        queue_name = self.QUEUE_PREFIX + (queue_name or None)
        return self.DB.llen(queue_name)

    def queues(self):
        """Return list of active queues
        """
        return [queue_name[len(self.QUEUE_PREFIX):] for queue_name in self.DB.keys('%s*' % self.QUEUE_PREFIX) if
                ':last_in' not in queue_name and ':last_out' not in queue_name]

    def push(self, proc, queue_name=None, max_size=1000000, **kwargs):
        """Push the given proc to the queue
        """
        queue_name = self.QUEUE_PREFIX + (queue_name or self.default_queue)
        self.DB.set(queue_name + ':last_in', pickle.dumps(datetime.datetime.now()))
        serialized_data = pickle.dumps([proc, kwargs])
        self.DB.lpush(queue_name, serialized_data)
        self.DB.ltrim(queue_name, 0, max_size)

    def pop(self, queue_name=None, timeout=0):
        """Pop the proc and associated parms from the given queue
        """
        queue_name = self.QUEUE_PREFIX + (queue_name or self.default_queue)
        self.DB.set(queue_name + ':last_out', pickle.dumps(datetime.datetime.now()))
        response = self.DB.brpop([queue_name], timeout)
        if response != None and len(response) == 2:
            serialized_data = response[1]
            if serialized_data != None:
                proc, kwargs = pickle.loads(serialized_data)
                return proc(**kwargs)
        return None

    def get_timestamps(self, queue_name=None):
        """Return tuple last_in:last_out for the given queue
        """
        none = pickle.dumps(None)
        queue_name = self.QUEUE_PREFIX + (queue_name or self.default_queue)
        return pickle.loads(self.DB.get(queue_name + ':last_in') or none), \
               pickle.loads(self.DB.get(queue_name + ':last_out') or none)


# Emacs:
# Local variables:
# time-stamp-pattern: "100/^__updated__ = '%%'$"
# End:
