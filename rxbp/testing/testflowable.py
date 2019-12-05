from typing import Any, Dict

import rxbp
from rxbp.flowablebase import FlowableBase
from rxbp.observable import Observable
from rxbp.selectors.bases import Base

from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription, SubscriptionInfo


class TestFlowable(FlowableBase):
    def __init__(self, base: Base = None, selectors: Dict[Base, Observable] = None, subscriber: Subscriber = None):
        super().__init__()

        self.base = base
        self.selectors = selectors
        self.subscriber = subscriber

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        self.subscriber = subscriber

        return Subscription(info=SubscriptionInfo(base=self.base, selectors=self.selectors), observable=rxbp.empty())
