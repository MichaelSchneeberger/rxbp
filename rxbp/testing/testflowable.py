from typing import Dict

from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observable import Observable
from rxbp.indexed.selectors.flowablebase import FlowableBase
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.testing.testobservable import TestObservable


class TestFlowable(FlowableMixin):
    def __init__(
            self,
            base: FlowableBase = None,
            selectors: Dict[FlowableBase, Observable] = None,
            subscriber: Subscriber = None,
            observable: Observable = None,
    ):
        super().__init__()

        self.base = base
        self.selectors = selectors
        self.subscriber = subscriber
        self.observable = observable or TestObservable()

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        self.subscriber = subscriber

        return init_subscription(
            # info=BaseAndSelectors(base=self.base, selectors=self.selectors),
            observable=self.observable,
        )

    def on_next_single(self, v):
        return self.observable.on_next_single(v)

    def on_next_list(self, v):
        return self.observable.on_next_list(v)

    def on_error(self, exc):
        return self.observable.on_error(exc)

    def on_completed(self):
        return self.observable.on_completed()
