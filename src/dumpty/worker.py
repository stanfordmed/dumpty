

from queue import Queue
from threading import Thread
from typing import Callable


class Worker(Thread):
    def __init__(self, func: Callable, in_queue: Queue, out_queue: Queue):
        self.func = func
        self.in_queue = in_queue
        self.out_queue = out_queue
        super().__init__()
        self.daemon = True
        self.start()

    def run(self):
        while True:
            item = self.in_queue.get()
            if isinstance(item, (Exception)):
                self.out_queue.put(item)
                self.in_queue.task_done()
            else:
                try:
                    result = self.func(item)
                except Exception as ex:
                    result = ex
                finally:
                    self.out_queue.put(result)
                    self.in_queue.task_done()
