from typing import Callable, Any, Set

from rx.disposable import Disposable

from rxbp.observable import Observable
from rxbp.subscriber import Subscriber
from rxbp.flowable import Flowable
from rxbp.flowablebase import FlowableBase


class AnonymousFlowable(Flowable):
    def __init__(self,
                 unsafe_subscribe_func: Callable[[Subscriber], Flowable.FlowableReturnType],
                 base: Any = None,
                 selectable_bases: Set[Any] = None):

        class InnerSubscriptable(FlowableBase):
            def unsafe_subscribe(self, subscriber: Subscriber) -> Flowable.FlowableReturnType:
                return unsafe_subscribe_func(subscriber)

        subscriptable_base = InnerSubscriptable(base=base, selectable_bases=selectable_bases)

        super().__init__(subscriptable=subscriptable_base)
