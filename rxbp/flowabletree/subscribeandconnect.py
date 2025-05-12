from dataclasses import dataclass

from continuationmonad.typing import ContinuationCertificate

from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.state import State
from rxbp.flowabletree.nodes import FlowableNode
from rxbp.flowabletree.sources.connectable import ConnectableFlowableNode
from rxbp.flowabletree.subscription import Subscription


def subscribe_and_connect(
    subscriptions: tuple[Subscription, ...],
    state: State,
    connections: dict[ConnectableFlowableNode, FlowableNode] | None = None,
):
    if connections:
        state = state.copy(connections=state.connections | connections)

    # discover sharable nodes
    for subscription in subscriptions:
        state = subscription.discover(state)

    # assign weights
    for subscription in subscriptions:
        state = subscription.assign_weights(state, 1)

    for connectable in state.discovered_connectables:
        state = state.connections[connectable].assign_weights(state, 1)

    # subscribe
    for subscription in subscriptions:
        state = subscription.apply(state)

    while state.discovered_subscriptions:
        discovered_subscriptions = state.discovered_subscriptions
        state = state.copy(discovered_subscriptions={})

        for subscription in discovered_subscriptions:
            state = subscription.subscribe(state)

    return state


def subscribe_single_sink(
    source: FlowableNode,
    sink: Observer,
    state: State,
    weight: int,
    connections: dict[ConnectableFlowableNode, FlowableNode] | None = None,
):
    @dataclass
    class SinlgeSinkSubscription(Subscription):
        result: SubscriptionResult | None

        def discover(self, state: State):
            return source.discover(state)

        def assign_weights(self, state: State, weight: int):
            return source.assign_weights(state, weight)

        def apply(self, state: State):
            state, self.result = source.unsafe_subscribe(
                state,
                args=SubscribeArgs(
                    observer=sink,
                    weight=weight,
                ),
            )
            return state
        
    subscription = SinlgeSinkSubscription(result=None)
        
    state = subscribe_and_connect(
        subscriptions=(subscription,),
        connections=connections,
        state=state,
    )

    assert subscription.result is not None

    return state, subscription.result




# def subscribe_and_connect(
#     subscriptions: tuple[Subscription, ...],
#     connections: dict[ConnectableFlowableNode, FlowableNode],
#     state: State,
# ):
#     results = []

#     state = state.copy(connections=connections)

#     # discover sharable nodes
#     for subscription in subscriptions:
#         state = subscription.discover(state)

#     # assign weights
#     for subscription in subscriptions:
#         state = subscription.assign_weights(state, 1)

#     for connectable in state.discovered_connectables:
#         state = state.connections[connectable].assign_weights(state, 1)

#     # subscribe
#     for subscription in subscriptions:
#         state, result = subscription.apply(state)
#         results.append(result)

#     while state.connectable_observers:
#         connectable_observers = state.connectable_observers
#         state = state.copy(connectable_observers={})

#         for connectable, observer in connectable_observers.items():
#             state = observer.connect(state, state.connections[connectable])

#     return state, results
