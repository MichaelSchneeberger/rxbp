from typing import Callable, Any, Set

from rxbp.selectors.bases import Base
from rxbp.subscriber import Subscriber
from rxbp.flowable import Flowable
from rxbp.flowablebase import FlowableBase


class AnonymousFlowable(Flowable):
    def __init__(self, unsafe_subscribe_func: Callable[[Subscriber], Flowable.FlowableReturnType],
                 base: Base = None,
                 selectable_bases: Set[Base] = None):

        class InnerSubscriptable(FlowableBase):
            def unsafe_subscribe(self, subscriber: Subscriber) -> Flowable.FlowableReturnType:
                return unsafe_subscribe_func(subscriber)

        flowable = InnerSubscriptable(base=base, selectable_bases=selectable_bases)
        super().__init__(flowable=flowable)
