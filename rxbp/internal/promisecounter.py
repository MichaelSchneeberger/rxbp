import threading

import rx

from rxbp.ack import Ack


class PromiseCounter:
    def __init__(self, value, initial):
        self.lock = threading.RLock()

        self.value = value
        self.counter = initial
        self.promise = Ack()

    def acquire(self):
        with self.lock:
            self.counter += 1

    def countdown(self):
        with self.lock:
            self.counter -= 1
            counter = self.counter

        if counter == 0:
            rx.just(self.value).subscribe(self.promise)

