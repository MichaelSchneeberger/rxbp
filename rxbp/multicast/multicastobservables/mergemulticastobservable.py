import threading
from dataclasses import dataclass
from typing import Iterable

import rx
from rx.disposable import CompositeDisposable, SingleAssignmentDisposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastobservers.mergemulticastobserver import MergeMultiCastObserver
from rxbp.scheduler import Scheduler


@dataclass
class MergeMultiCastObservable(MultiCastObservable):
    sources: Iterable[MultiCastObservable]
    # scheduler: Scheduler

    def observe(self, observer_info: MultiCastObserverInfo) -> rx.typing.Disposable:
        group = CompositeDisposable()
        lock = threading.RLock()

        for inner_source in self.sources:
            inner_subscription = SingleAssignmentDisposable()
            group.add(inner_subscription)

            disposable = inner_source.observe(observer_info.copy(
                observer=MergeMultiCastObserver(
                    observer=observer_info.observer,
                    lock=lock,
                    inner_subscription=inner_subscription,
                    group=group
                )
            ))
            inner_subscription.disposable = disposable

        return group
