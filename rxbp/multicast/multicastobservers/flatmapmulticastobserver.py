import threading
from dataclasses import dataclass
from typing import List, Callable, Any

from rx.disposable import CompositeDisposable, SingleAssignmentDisposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastobservers.innerflatmapmulticastobserver import InnerFlatMapMultiCastObserver
from rxbp.multicast.typing import MultiCastItem
from rxbp.scheduler import Scheduler


@dataclass
class FlatMapMultiCastObserver(MultiCastObserver):
    observer_info: MultiCastObserverInfo
    func: Callable[[Any], MultiCastObservable]
    lock: threading.RLock
    state: List[int]
    composite_disposable: CompositeDisposable
    multicast_scheduler: Scheduler

    def on_next(self, item: MultiCastItem) -> None:
        if isinstance(item, list):
            elements = item
        else:
            try:
                # materialize received values immediately
                elements = list(item)
            except Exception as exc:
                self.on_error(exc)
                return

        if len(elements) == 0:
            return

        def subscribe_action(_, __):
            try:
                for elem in elements:
                    with self.lock:
                        self.state[0] += 1

                    inner_subscription = SingleAssignmentDisposable()

                    disposable = self.func(elem).observe(self.observer_info.copy(
                        observer=InnerFlatMapMultiCastObserver(
                            observer=self.observer_info.observer,
                            lock=self.lock,
                            state=self.state,
                            composite_disposable=self.composite_disposable,
                            inner_subscription=inner_subscription,
                        ),
                    ))
                    inner_subscription.disposable = disposable
                    self.composite_disposable.add(inner_subscription)

            except Exception as exc:
                self.on_error(exc)
                return

        if self.multicast_scheduler.idle:
            disposable = self.multicast_scheduler.schedule(subscribe_action)
            self.composite_disposable.add(disposable)
        else:
            subscribe_action(None, None)

    def on_error(self, exc: Exception) -> None:
        self.observer_info.observer.on_error(exc)

    def on_completed(self) -> None:
        with self.lock:
            meas_state = self.state[0] - 1
            self.state[0] = meas_state

        if meas_state == 0:
            self.observer_info.observer.on_completed()