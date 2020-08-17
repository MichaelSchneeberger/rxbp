from dataclasses import dataclass
from traceback import FrameSummary
from typing import List

from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablebasemixin import FlowableBaseMixin
from rxbp.observables.firstobservable import FirstObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.utils.tooperatorexception import to_operator_exception


@dataclass
class FirstFlowable(FlowableBaseMixin):
    source: FlowableBaseMixin
    stack: List[FrameSummary]

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        try:
            subscription = self.source.unsafe_subscribe(subscriber=subscriber)
            return subscription.copy(observable=FirstObservable(
                source=subscription.observable,
                stack=self.stack,
            ))

        except Exception:
            raise Exception(to_operator_exception(
                message=f'something went wrong when subscribing to {self.source}',
                stack=self.stack,
            ))