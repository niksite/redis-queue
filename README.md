redis-queue
===========

Simple and easy to use implementation of tasks queue, based on python and redis database.

An example of how I using it. I have the following cronjob in mine's crontab:

    */20 * * * * redis_queue/tools/placer.py apps.mystats.tasks update_stats -q maint

It means that usual python function update_stats() from module mystats will be placed to 'maint' queue every 20 minutes.

If we have the 'redis_queue/tools/worker.py -q maint' process running, the 'update_stats()' function will be pop`ed from the queue and executed.

I'm using a supervisor application to handle mine's workers bunch.

Indeed, update_stats() function can place some more tasks to any queue during it's work, it is as easy as:

```python
from redis_queue import Queue

def update_feed(feed_id):
    # do something useful
    return True

def update_stats():
    q = Queue()
    for feed_id in range(1000):
        q.push(update_feed, feed_id=feed_id, queue_name='hi')
```

This time, update_feed(feed_id) function has been placed to the 'hi' queue 1000 times.
