

from queue import Queue
from random import uniform
from threading import Thread
from time import sleep
from typing import Callable


class Worker(Thread):

    def __init__(self, func: Callable, in_queue: Queue, out_queue: Queue):
        self.func = func
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.busy = False
        super().__init__()
        self.daemon = True
        self.start()

    def run(self):
        while True:
            item = self.in_queue.get()
            if isinstance(item, (Exception)):
                self.in_queue.task_done()
                self.out_queue.put(item)
            else:
                try:
                    self.busy = True
                    result = self.func(item)
                except Exception as ex:
                    result = ex
                finally:
                    self.busy = False
                    self.in_queue.task_done()
                    self.out_queue.put(result)


class WorkerPool():
    def __init__(self, func: Callable, in_queue: Queue, out_queue: Queue, size: int):
        self.func = func
        self.in_queue = in_queue
        self.out_queue = out_queue

        self.workers = []
        for _ in range(size):
            self.workers.append(Worker(func, in_queue, out_queue))

    def busy_count(self):
        return sum(worker.busy for worker in self.workers)


class Submitter(Thread):
    """Submits items to a queue, waiting for a random short interval to avoid bursting
    """

    def __init__(self, items, queue: Queue):
        self.items = items
        self.queue = queue
        super().__init__()
        self.daemon = True
        self.start()

    def run(self):
        for item in self.items:
            self.queue.put(item)
            sleep(uniform(0.0, 0.25))
