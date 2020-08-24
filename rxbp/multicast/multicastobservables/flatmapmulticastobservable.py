import threading
from dataclasses import dataclass
from typing import Callable, Any

from rx.disposable import Disposable, CompositeDisposable, SingleAssignmentDisposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.typing import MultiCastItem


@dataclass
class FlatMapMultiCastObservable(MultiCastObservable):
    source: MultiCastObservable
    func: Callable[[Any], MultiCastObservable]

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        outer_self = self

        @dataclass
        class InnerObserver(MultiCastObserver):
            observer: MultiCastObserver
            lock: threading.RLock
            state: int
            composite_disposable: CompositeDisposable
            inner_subscription: SingleAssignmentDisposable

            def on_next(self, elem: MultiCastItem) -> None:
                try:
                    self.observer.on_next(elem)
                except:
                    # print(elem)
                    # print(self.observer)
                    # print(self.observer.on_next)
                    raise

            def on_error(self, exc: Exception) -> None:
                self.observer.on_error(exc)

            def on_completed(self) -> None:
                self.composite_disposable.remove(self.inner_subscription)
                with self.lock:
                    meas_state = self.state - 1
                    self.state = meas_state

                if meas_state == 0:
                    self.observer.on_completed()

        @dataclass
        class FlatMapMultiCastObserver(MultiCastObserver):
            lock: threading.RLock
            state: int
            composite_disposable: CompositeDisposable

            def on_next(self, item: MultiCastItem) -> None:
                with self.lock:
                    self.state += 1

                for elem in item:
                    inner_subscription = SingleAssignmentDisposable()
                    disposable = outer_self.func(elem).observe(observer_info.copy(
                        observer=InnerObserver(
                            observer=observer_info.observer,
                            lock=self.lock,
                            state=self.state,
                            composite_disposable=self.composite_disposable,
                            inner_subscription=inner_subscription,
                        ),
                    ))
                    inner_subscription.disposable = disposable
                    self.composite_disposable.add(inner_subscription)

            def on_error(self, exc: Exception) -> None:
                observer_info.observer.on_error(exc)

            def on_completed(self) -> None:
                with self.lock:
                    meas_state = self.state

                if meas_state == 0:
                    observer_info.observer.on_completed()

        return self.source.observe(observer_info.copy(
            observer=FlatMapMultiCastObserver(
                lock=threading.RLock(),
                state=0,
                composite_disposable=CompositeDisposable(),
            )
        ))

