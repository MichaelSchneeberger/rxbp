import threading
from dataclasses import dataclass
from typing import Callable, Any, Tuple, Optional

from rx.disposable import Disposable, CompositeDisposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastobservers.flatmapmulticastobserver import FlatMapMultiCastObserver
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber


@dataclass
class FlatMapMultiCastObservable(MultiCastObservable):
    source: MultiCastObservable
    func: Callable[[Any], Tuple[MultiCastObservable, Optional[MultiCastSubscriber]]]

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        return self.source.observe(observer_info.copy(
            observer=FlatMapMultiCastObserver(
                observer_info=observer_info,
                func=self.func,
                lock=threading.RLock(),
                state=[1],
                composite_disposable=CompositeDisposable(),
            )
        ))

