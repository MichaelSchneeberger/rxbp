import threading
from dataclasses import dataclass
from typing import List

from rx.disposable import CompositeDisposable, SingleAssignmentDisposable

from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.typing import MultiCastItem


@dataclass
class InnerFlatMapMultiCastObserver(MultiCastObserver):
    observer: MultiCastObserver
    lock: threading.RLock
    state: List[int]
    composite_disposable: CompositeDisposable
    inner_subscription: SingleAssignmentDisposable

    def on_next(self, item: MultiCastItem) -> None:
        self.observer.on_next(item)

    def on_error(self, exc: Exception) -> None:
        self.observer.on_error(exc)

    def on_completed(self) -> None:
        self.composite_disposable.remove(self.inner_subscription)
        with self.lock:
            meas_state = self.state[0] - 1
            self.state[0] = meas_state

        if meas_state == 0:
            self.observer.on_completed()
