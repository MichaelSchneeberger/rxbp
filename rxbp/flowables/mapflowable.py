from dataclasses import dataclass
from traceback import FrameSummary
from typing import Callable, Any, List

from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.mapobservable import MapObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.typing import ValueType
from rxbp.utils.tooperatorexception import to_operator_exception


@dataclass
class MapFlowable(FlowableMixin):
    source: FlowableMixin
    func: Callable[[ValueType], Any]
    # stack: List[FrameSummary]

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        # try:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)
        return subscription.copy(
            observable=MapObservable(
                source=subscription.observable,
                func=self.func,
            ),
        )

        # except AttributeError:
        #     raise Exception(to_operator_exception(
        #         message=f'something went wrong when subscribing to {self.source}',
        #         stack=self.stack,
        #     ))
