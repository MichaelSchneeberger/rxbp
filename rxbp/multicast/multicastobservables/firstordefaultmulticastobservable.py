from dataclasses import dataclass
from typing import Callable, Any

from rx.disposable import Disposable, SingleAssignmentDisposable, CompositeDisposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastobservers.firstordefaultmulticastobserver import FirstOrDefaultMultiCastObserver


@dataclass
class FilterOrDefaultMultiCastObservable(MultiCastObservable):
    source: MultiCastObservable
    lazy_val: Callable[[], Any]

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        single_assign_disposable = SingleAssignmentDisposable()
        disposable = self.source.observe(observer_info.copy(
            observer = FirstOrDefaultMultiCastObserver(
                source=observer_info.observer,
                lazy_val=self.lazy_val,
            ),
        ))
        return CompositeDisposable(single_assign_disposable, disposable)
