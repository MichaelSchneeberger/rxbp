from abc import abstractmethod
from dataclasses import dataclass

from rxbp.flowabletree.nodes import FlowableNode
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.state import State
from rxbp.flowabletree.assignweightmixin import AssignWeightMixin


class Subscription(AssignWeightMixin):
    @abstractmethod
    def apply(self, state: State) -> tuple[State, SubscriptionResult]: ...


@dataclass
class StandardSubscription(Subscription):
    source: FlowableNode
    sink: Observer
    
    def discover(self, state: State):
        return self.source.discover(state)

    def assign_weights(self, state: State, weight: int):
        return self.source.assign_weights(state, weight)
    
    def apply(self, state: State):
        return self.source.unsafe_subscribe(
            state,
            args=SubscribeArgs(
                observer=self.sink,
                schedule_weight=1,
            ),
        )
