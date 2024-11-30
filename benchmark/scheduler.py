import heapq
import random
import time
import threading


class Scheduler(threading.Thread):
    def __init__(self):
        super().__init__()
        self.lock = threading.Condition()
        self.app_manger = None
        self.heap = []

    def set_app(self, application_manager):
        self.app_manger = application_manager

    def run(self):
        while True:
            with self.lock:
                while True:
                    if not self.heap:
                        self.lock.wait()
                        continue
                    now = time.time_ns()
                    (trans_due, _, terminal) = self.heap[0]
                    if trans_due > now:
                        self.lock.wait(trans_due - now)
                        continue
                    heapq.heappop(self.heap)
                    break

            self.app_manger.queue_append(terminal)
            if terminal.is_finish():
                break

    def at(self, trans_due, terminal):
        with self.lock:
            sched_fuzz = random.randint(0, 999999999)
            while (trans_due, sched_fuzz, terminal) in self.heap:
                sched_fuzz = random.randint(0, 999999999)
            heapq.heappush(
                self.heap, (trans_due, sched_fuzz, terminal))
            self.lock.notify()
