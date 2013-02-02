#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Description:
    Scheduler process: parse files, retrieve periodical tasks, then run them in appropriate time.
    You should run only one scheduler process.

History:
   * 2010-03-25T09:17:55+0300 Initial commit. Version 0.1 released.
"""

__author__ = 'Nikolay Panov (author@niksite.ru)'
__license__ = 'GPLv3'
__version__ = 0.1
__updated__ = '2010-04-01 14:57:47 nik'


import sys
import optparse
import glob
import os.path
import logging

# from redis_queue import Queue


def scan_file(filename):
    """Scan the given file for periodical tasks
    return list of periodical tasks

    @filename - name of file to scan"""
    filedir, filebasename = os.path.dirname(filename), os.path.splitext(os.path.basename(filename))[0]
    if filedir not in sys.path:
        sys.path.append(filedir)
        cleanup_required = True
    else:
        cleanup_required = False
    try:
        module = __import__(filebasename, {}, {}, [])
    except ImportError:
        logging.error('Cannot import "%s" module from "%s"' % (filebasename, filedir))
        return []
    finally:
        if cleanup_required:
            sys.path.remove(filedir)
    for func in module.__dict__.values():
        if 'function' in str(type(func)):
            if hasattr(func, 'run_every'):
                # TODO
                pass
    return []


def scheduler(target_files, sleep_time):
    """Scheduler process: parse files, retrieve periodical tasks, then run them in appropriate time.

    @target_files - a list of files which should be scanned for periodical tasks
    @sleep_time - How often scheduler will poll the schedule"""
    # Step 1: scanning
    for target_file_glob in target_files:
        for target_file in glob.glob(target_file_glob):
            scan_file(target_file)


if __name__ == "__main__":
    """Starts the scheduler main loop."""
    parser = optparse.OptionParser("""usage: %prog [options] <files to scan>
    where <files to scan> is a list of files which should be scanned
    for periodical tasks, wildcards are available to use
    """)
    parser.add_option('-s', '--sleep-time', default=60,
                      action="store", dest="sleep_time", type="int",
                      help="How often scheduler will poll the schedule (default: every 60 seconds).")
    options, args = parser.parse_args(sys.argv[1:])
    if len(args) < 1:
        parser.error("Nothing to scan, exiting.")
    options.__dict__['target_files'] = args
    scheduler(**vars(options))

# Emacs:
# Local variables:
# time-stamp-pattern: "100/^__updated__ = '%%'$"
# End:
