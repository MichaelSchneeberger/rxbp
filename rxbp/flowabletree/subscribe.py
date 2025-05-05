from rxbp.flowabletree.subscription import Subscription
from rxbp.state import State
from rxbp.flowabletree.nodes import FlowableNode
from rxbp.flowabletree.sources.connectable import ConnectableFlowableNode


def subscribe(
    subscriptions: tuple[Subscription, ...],
    connections: dict[ConnectableFlowableNode, FlowableNode],
    state: State,
):
    results = []

    state = state.copy(connections=connections)

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
        state, result = subscription.apply(state)
        results.append(result)

    while state.connectable_observers:
        connectable_observers = state.connectable_observers
        state = state.copy(connectable_observers={})

        for connectable, observer in connectable_observers.items():
            state = observer.connect(state, state.connections[connectable])

    return state, results
