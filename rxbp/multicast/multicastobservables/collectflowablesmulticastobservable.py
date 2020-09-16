from dataclasses import dataclass
from traceback import FrameSummary
from typing import List

import rx
from rx.disposable import RefCountDisposable, SingleAssignmentDisposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastobservers.collectflowablesmulticastobserver import CollectFlowablesMultiCastObserver
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber


@dataclass
class CollectFlowablesMultiCastObservable(MultiCastObservable):
    source: MultiCastObservable
    maintain_order: bool
    stack: List[FrameSummary]
    subscriber: MultiCastSubscriber

    def observe(self, observer_info: MultiCastObserverInfo) -> rx.typing.Disposable:
        disposable = SingleAssignmentDisposable()

        # dispose source if MultiCast sink gets disposed and all inner Flowable sinks
        # are disposed
        ref_count_disposable = RefCountDisposable(disposable)

        disposable.disposable = self.source.observe(observer_info.copy(
            observer=CollectFlowablesMultiCastObserver(
                next_observer=observer_info.observer,
                ref_count_disposable=ref_count_disposable,
                maintain_order=self.maintain_order,
                stack=self.stack,
                subscriber=self.subscriber,
            ),
        ))

        return ref_count_disposable