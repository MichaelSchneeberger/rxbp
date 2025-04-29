from rxbp.flowabletree.nodes import FlowableNode
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.state import State


def subscribe(
    sources: tuple[FlowableNode, ...],
    sinks: tuple[SubscribeArgs, ...],
    state: State,
):
    results = []

    # discover sharable nodes
    for source in sources:
        state = source.discover(state)

    # assign weights
    for source in sources:
        state = source.assign_weights(state, 1)

    for sink in state.connections.values():
        state = sink.assign_weights(state, 1)

    # subscribe
    for source, args in zip(sources, sinks):
        state, result = source.unsafe_subscribe(
                state,
                args=args,
        )
        results.append(result)

    while state.connectable_observers:
        for connectable, observer in state.connectable_observers.items():
            # state = state.copy(connectable_observers={})
            state, result = connectable.unsafe_subscribe(
                state=state.copy(connectable_observers={}),
                # state.copy(certificate=result.certificate),
                args=SubscribeArgs(
                    observer=observer,
                    schedule_weight=1,
                ),
            )
            observer.certificate = result.certificate

    return results