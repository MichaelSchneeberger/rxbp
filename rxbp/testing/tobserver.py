from dataclasses import dataclass
import continuationmonad
from continuationmonad.typing import (
    Scheduler,
    DeferredHandler,
    ContinuationCertificate,
)

from rxbp.flowabletree.observer import Observer


@dataclass
class TObserver[V](Observer):
    received: list[V]
    certificate: ContinuationCertificate
    is_completed: bool

    # def __init__(self, certificate: ContinuationCertificate):
    #     self.received = []
    #     self.deferred_observer = None
    #     self.is_completed = False
    #     self.exception = None

    #     # counts the number of times `on_next` is called
    #     self.on_next_counter = 0

    #     self.certificate = certificate

    # def request(self):
    #     return self.deferred_observer.on_next(None)

    def on_next(self, item: V):
        # self.on_next_counter += 1
        self.received.append(item)

        return continuationmonad.from_(None)
    
        # def on_next_subscription(_, deferred_observer: DeferredHandler):
        #     self.deferred_observer = deferred_observer
        #     return self.certificate

        # return continuationmonad.defer(on_next_subscription)

    def on_next_and_complete(self, item: V):
        self.received.append(item)
        return continuationmonad.from_(self.certificate)

    def on_error(self, err):
        self.exception = err
        return continuationmonad.from_(self.certificate)

    def on_completed(self):
        self.is_completed = True
        return continuationmonad.from_(self.certificate)


def init_test_observer():
    return TObserver(
        received=[],
        certificate=None,
        is_completed=False,
    )
