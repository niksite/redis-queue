#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Description:
    this script just place the given function to appropriate queue (useful with cron)
    Example:
    ./placer.py apps.tasks test x=100.00

History:
   * 2010-03-25T09:17:55+0300 Initial commit. Version 0.1 released.
"""

__author__ = 'Nikolay Panov (author@niksite.ru)'
__license__ = 'GPLv3'
__version__ = 0.1
__updated__ = '2010-10-12 14:29:03 nik'


import sys
import optparse
import types
import re

from redis_queue import Queue


if __name__ == "__main__":
    """Place (e.g. by cron) a task from this file to a queue."""

    option_list = (
        optparse.make_option('-q', '--queue', default='default',
            action="store", dest="queue_name",
            help="Name of the queue to place the given task."),
        )

    parser = optparse.OptionParser(
        option_list=option_list,
        usage='Usage: %prog <module> <proc name> [<proc args>] -q <queue_name>')
    options, values = parser.parse_args(sys.argv[1:])

    if len(values) < 2:
        parser.error('missed module or proc name')

    modulename, procname = values[:2]
    module = __import__(modulename, {}, {}, [procname])
    if procname in module.__dict__ and \
       isinstance(module.__dict__[procname], types.FunctionType):
        proc = module.__dict__[procname]
    else:
        parser.error('function with name %s is not found in this file' % procname)

    params = {}
    for value in values[2:]:
        key, val = value.split('=')
        if re.match('^\d+$', val):
            val = int(val)
        elif re.match('^[\d\.]+$', val):
            val = float(val)
        params[key] = val

    Queue().push(proc, queue_name=options.queue_name, **params)

# Emacs:
# Local variables:
# time-stamp-pattern: "100/^__updated__ = '%%'$"
# End:
