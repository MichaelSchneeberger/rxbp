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
    is_backpressured: bool
    handler: DeferredHandler

    def request(self):
        certificate = self.handler.resume(continuationmonad.init_trampoline(), None)
        self.certificate = certificate

    def on_next(self, item: V):
        self.received.append(item)

        if self.is_backpressured:
            def on_next_subscription(_, handler: DeferredHandler):
                self.handler = handler
                return self.certificate

            return continuationmonad.defer(on_next_subscription)
        
        else:
            return continuationmonad.from_(None)

    def on_next_and_complete(self, item: V):
        self.received.append(item)
        self.is_completed = True
        return continuationmonad.from_(self.certificate)

    def on_error(self, err):
        self.exception = err
        return continuationmonad.from_(self.certificate)

    def on_completed(self):
        self.is_completed = True
        return continuationmonad.from_(self.certificate)


def init_test_observer(is_backpressured: bool | None = None):
    if is_backpressured is None:
        is_backpressured = False

    return TObserver(
        received=[],
        certificate=None,
        is_completed=False,
        is_backpressured=is_backpressured,
        handler=None,
    )
