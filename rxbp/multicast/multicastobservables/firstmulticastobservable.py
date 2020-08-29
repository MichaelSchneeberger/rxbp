from dataclasses import dataclass
from traceback import FrameSummary
from typing import List

import rx
from rx.disposable import SingleAssignmentDisposable, CompositeDisposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastobservers.firstmulticastobserver import FirstMultiCastObserver


@dataclass
class FirstMultiCastObservable(MultiCastObservable):
    source: MultiCastObservable
    stack: List[FrameSummary]

    def observe(self, observer_info: MultiCastObserverInfo) -> rx.typing.Disposable:
        single_assign_disposable = SingleAssignmentDisposable()
        disposable = self.source.observe(observer_info.copy(
            observer=FirstMultiCastObserver(
                source=observer_info.observer,
                disposable=single_assign_disposable,
                stack=self.stack,
            )
        ))
        return CompositeDisposable(single_assign_disposable, disposable)
