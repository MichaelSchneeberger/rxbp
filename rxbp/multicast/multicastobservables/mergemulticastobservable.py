import threading
from dataclasses import dataclass
from typing import Iterable

import rx
from rx.disposable import CompositeDisposable, SingleAssignmentDisposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.typing import MultiCastItem
from rxbp.scheduler import Scheduler


@dataclass
class MergeMultiCastObservable(MultiCastObservable):
    sources: Iterable[MultiCastObservable]
    scheduler: Scheduler

    def observe(self, observer_info: MultiCastObserverInfo) -> rx.typing.Disposable:
        group = CompositeDisposable()
        m = SingleAssignmentDisposable()
        group.add(m)
        lock = threading.RLock()

        @dataclass
        class MergeMultiCastObserver(MultiCastObserver):
            observer: MultiCastObserver
            lock: threading.RLock()
            inner_subscription: SingleAssignmentDisposable

            def on_next(self, elem: MultiCastItem) -> None:
                # print(elem)
                # with self.lock:
                self.observer.on_next(elem)

            def on_error(self, exc: Exception) -> None:
                # with self.lock:
                self.observer.on_error(exc)

            def on_completed(self) -> None:
                with self.lock:
                    group.remove(self.inner_subscription)
                    if len(group) == 1:
                        self.observer.on_completed()

        for inner_source in self.sources:
            inner_subscription = SingleAssignmentDisposable()
            group.add(inner_subscription)

            disposable = inner_source.observe(observer_info.copy(
                observer=MergeMultiCastObserver(
                    observer=observer_info.observer,
                    lock=lock,
                    inner_subscription=inner_subscription,
                )
            ))
            inner_subscription.disposable = disposable

        return group
