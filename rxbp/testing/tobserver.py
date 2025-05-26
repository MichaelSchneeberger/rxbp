from dataclasses import dataclass

from donotation import do

import continuationmonad
from continuationmonad.typing import (
    DeferredHandler,
    ContinuationCertificate,
    MainVirtualTimeScheduler,
)

from rxbp.typing import Cancellable
from rxbp.flowabletree.observer import Observer


@dataclass
class TObserver[V](Observer):
    name: str | None
    received: list[V]
    # certificate: ContinuationCertificate
    cancellable: Cancellable
    is_completed: bool
    # is_backpressured: bool
    handler: DeferredHandler
    scheduler: MainVirtualTimeScheduler
    request_delay: float

    def _stop(self):
        return self.scheduler.stop(weight=1)

    def cancel(self):
        self.cancellable.cancel(self._stop())

    # def request(self, weight: int):
    #     trampoline = continuationmonad.init_trampoline()
    #     def trampoline_task():
    #         return self.handler.resume(trampoline, weight, None)
        
    #     self.certificate = trampoline.start_loop(trampoline_task, weight=weight, cancellation=None)

    @do()
    def on_next(self, item: V):
        self.received.append(item)

        # print(self.request_delay)

        if self.request_delay:
            yield continuationmonad.sleep(seconds=self.request_delay, scheduler=self.scheduler)
            # def on_next_subscription(_, handler: DeferredHandler):
            #     self.handler = handler
            #     return self.certificate

            # return continuationmonad.defer(on_next_subscription)
            return continuationmonad.from_(None)
        
        else:
            return continuationmonad.from_(None)

    def on_next_and_complete(self, item: V):
        self.received.append(item)
        self.is_completed = True

        # assert isinstance(self.certificate, ContinuationCertificate)
        return continuationmonad.from_(self._stop())

    def on_error(self, err):
        self.exception = err

        # assert isinstance(self.certificate, ContinuationCertificate)
        return continuationmonad.from_(self._stop())

    def on_completed(self):
        self.is_completed = True

        # assert isinstance(self.certificate, ContinuationCertificate)
        return continuationmonad.from_(self._stop())


def init_test_observer(
    scheduler: MainVirtualTimeScheduler,
    name: str | None = None,
    # is_backpressured: bool | None = None,
):
    # if is_backpressured is None:
    #     is_backpressured = False

    return TObserver(
        name=name,
        received=[],
        # certificate=None, # type: ignore
        cancellable=None, # type: ignore
        is_completed=False,
        # is_backpressured=is_backpressured,
        handler=None,
        scheduler=scheduler,
        request_delay=0,
    )
