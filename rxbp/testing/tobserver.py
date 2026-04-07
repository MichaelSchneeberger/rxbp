from dataclasses import dataclass

from donotation import do

import rxsingle
from rxsingle.typing import (
    DeferredHandler,
    ContCert,
    MainVirtualTimeScheduler,
)

from rxbp.typing import Cancellable
from rxbp.flowabletree.observer import Observer


@dataclass
class TObserver[V][U](Observer[U]):
    name: str | None
    received: list[V]
    # certificate: ContCert
    cancellable: Cancellable
    is_completed: bool
    # is_backpressured: bool
    handler: DeferredHandler
    scheduler: MainVirtualTimeScheduler
    request_delay: float

    def _stop(self):
        return self.scheduler.stop(virtual_count=1)

    def cancel(self):
        self.cancellable.cancel(self._stop())

    # def request(self, virtual_count: int):
    #     trampoline = contcert.init_trampoline()
    #     def trampoline_task():
    #         return self.handler.resume(trampoline, virtual_count, None)
        
    #     self.certificate = trampoline.start_loop(trampoline_task, virtual_count=virtual_count, cancellation=None)

    @do()
    def on_next(self, item: V):
        self.received.append(item)

        # print(self.request_delay)

        if self.request_delay:
            yield rxsingle.sleep(seconds=self.request_delay, scheduler=self.scheduler)
            # def on_next_subscription(_, handler: DeferredHandler):
            #     self.handler = handler
            #     return self.certificate

            # return rxsingle.defer(on_next_subscription)
            return rxsingle.from_(None)
        
        else:
            return rxsingle.from_(None)

    def on_next_and_complete(self, item: V):
        self.received.append(item)
        self.is_completed = True

        # assert isinstance(self.certificate, ContCert)
        return rxsingle.from_(self._stop())

    def on_error(self, err):
        self.exception = err

        # assert isinstance(self.certificate, ContCert)
        return rxsingle.from_(self._stop())

    def on_completed(self):
        self.is_completed = True

        # assert isinstance(self.certificate, ContCert)
        return rxsingle.from_(self._stop())


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
