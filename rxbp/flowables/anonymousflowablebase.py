from typing import Callable, Any, Set

from rxbp.selectors.bases import Base
from rxbp.subscriber import Subscriber
from rxbp.flowablebase import FlowableBase


class AnonymousFlowableBase(FlowableBase):
    def __init__(
            self,
            unsafe_subscribe_func: Callable[[Subscriber], FlowableBase.FlowableReturnType],
            base: Base = None,
            selectable_bases: Set[Base] = None,
    ):

        super().__init__(base, selectable_bases)

        class InnerSubscriptable(FlowableBase):
            def unsafe_subscribe(self, subscriber: Subscriber) -> FlowableBase.FlowableReturnType:
                return unsafe_subscribe_func(subscriber)

        flowable = InnerSubscriptable(base=base, selectable_bases=selectable_bases)

        self.unsafe_subscribe = flowable.unsafe_subscribe

    def unsafe_subscribe(self, subscriber: Subscriber) -> FlowableBase.FlowableReturnType:
        raise Exception('should not be called')