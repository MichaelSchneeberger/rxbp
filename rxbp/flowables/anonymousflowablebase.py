from typing import Callable

from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class AnonymousFlowableBase(FlowableMixin):
    def __init__(
            self,
            unsafe_subscribe_func: Callable[[Subscriber], Subscription],
    ):

        super().__init__()

        class InnerSubscriptable(FlowableMixin):
            def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
                return unsafe_subscribe_func(subscriber)

        flowable = InnerSubscriptable()

        self.unsafe_subscribe = flowable.unsafe_subscribe

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        raise Exception('should not be called')