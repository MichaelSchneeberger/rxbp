from typing import Any

import rxbp
from rxbp.flowablebase import FlowableBase

from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription, SubscriptionInfo


class TestFlowable(FlowableBase):
    def __init__(self, base: Any = None, subscriber: Subscriber = None):
        super().__init__()

        self.base = base
        self.subscriber = subscriber

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        self.subscriber = subscriber

        return Subscription(info=SubscriptionInfo(base=self.base), observable=rxbp.empty())
