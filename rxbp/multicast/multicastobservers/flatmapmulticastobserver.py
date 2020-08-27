import threading
from dataclasses import dataclass
from typing import List, Callable, Any

from rx.disposable import CompositeDisposable, SingleAssignmentDisposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastobservers.innerflatmapmulticastobserver import InnerFlatMapMultiCastObserver
from rxbp.multicast.typing import MultiCastItem


@dataclass
class FlatMapMultiCastObserver(MultiCastObserver):
    observer_info: MultiCastObserverInfo
    func: Callable[[Any], MultiCastObservable]
    lock: threading.RLock
    state: List[int]
    composite_disposable: CompositeDisposable

    def on_next(self, item: MultiCastItem) -> None:
        for elem in item:
            with self.lock:
                self.state[0] += 1

            inner_subscription = SingleAssignmentDisposable()

            try:
                disposable = self.func(elem).observe(self.observer_info.copy(
                    observer=InnerFlatMapMultiCastObserver(
                        observer=self.observer_info.observer,
                        lock=self.lock,
                        state=self.state,
                        composite_disposable=self.composite_disposable,
                        inner_subscription=inner_subscription,
                    ),
                ))

            except Exception as exc:
                self.on_error(exc)
                return

            inner_subscription.disposable = disposable
            self.composite_disposable.add(inner_subscription)

    def on_error(self, exc: Exception) -> None:
        self.observer_info.observer.on_error(exc)

    def on_completed(self) -> None:
        with self.lock:
            meas_state = self.state[0] - 1
            self.state[0] = meas_state

        if meas_state == 0:
            self.observer_info.observer.on_completed()