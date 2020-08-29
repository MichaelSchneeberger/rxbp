import threading
from dataclasses import dataclass
from typing import Callable, Any

from rx.disposable import Disposable, CompositeDisposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastobservers.flatmapmulticastobserver import FlatMapMultiCastObserver
from rxbp.scheduler import Scheduler


@dataclass
class FlatMapMultiCastObservable(MultiCastObservable):
    source: MultiCastObservable
    func: Callable[[Any], MultiCastObservable]
    multicast_scheduler: Scheduler

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        return self.source.observe(observer_info.copy(
            observer=FlatMapMultiCastObserver(
                observer_info=observer_info,
                func=self.func,
                lock=threading.RLock(),
                state=[1],
                composite_disposable=CompositeDisposable(),
                multicast_scheduler=self.multicast_scheduler
            )
        ))

