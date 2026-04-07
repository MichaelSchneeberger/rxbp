from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import override

from contcert.typing import ContCert, Scheduler

from rxbp.flowabletree.state import State
from rxbp.flowabletree.assignvirtualcount import AssignVirtualCount
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs, init_subscribe_args
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.flowablenode import FlowableNode
# from rxbp.flowabletree.sources.connectable import ConnectableFlowableNode


class Subscription[S](AssignVirtualCount[S], ABC):
    @abstractmethod
    def apply(self, state: S) -> tuple[S, SubscriptionResult]: ...


# @dataclass
# class StandardSubscription[U](Subscription):
#     source: FlowableNode[S, U]
#     sink: Observer[U]
#     virtual_count: int
#     scheduler: Scheduler
#
#     def discover(self, state: State):
#         return self.source.discover(state)
#
#     def assign_virtual_count(self, state: State, virtual_count: int):
#         return self.source.assign_virtual_count(state, virtual_count)
#
#     def apply(self, state: State):
#         return self.source.unsafe_subscribe(
#             state,
#             args=init_subscribe_args(
#                 observer=self.sink,
#                 virtual_count=self.virtual_count,
#                 scheduler=self.scheduler,
#             ),
#         )


def subscribe[S: State, U](
    subscriptions: tuple[Subscription[S], ...],
    state: S,
    # connections: dict[ConnectableFlowableNode[S, U], FlowableNode[S, U]] | None = None,
) -> S:
    # if connections:
    #     state = state.copy(connections=state.connections | connections)

    # discover sharable nodes
    for subscription in subscriptions:
        state = subscription.discover(state)

    # assign virtual_counts
    for subscription in subscriptions:
        state = subscription.assign_virtual_count(state, 1)

    # for connectable in state.discovered_connectables:
    #     state = state.connections[connectable].assign_virtual_count(state, 1)

    # subscribe
    for subscription in subscriptions:
        state, result = subscription.apply(state)

    # while state.discovered_subscriptions:
    #     discovered_subscriptions = state.discovered_subscriptions
    #     state = state.copy(discovered_subscriptions={})
    #
    #     for subscription in discovered_subscriptions:
    #         state = subscription.apply(state)

    return state


def subscribe_single_sink[S: State, U](
    source: FlowableNode[S, U],
    args: SubscribeArgs[U],
    state: S,
    # connections: dict[ConnectableFlowableNode[S, U], FlowableNode[S, U]] | None = None,
) -> tuple[S, SubscriptionResult]:
    @dataclass
    class SinlgeSinkSubscription(Subscription[S]):
        result: SubscriptionResult | None

        @override
        def discover(self, state: S) -> S:
            return source.discover(state)

        @override
        def assign_virtual_count(self, state: S, virtual_count: int) -> S:
            return source.assign_virtual_count(state, virtual_count)

        @override
        def apply(self, state: S) -> tuple[S, SubscriptionResult]:
            state, result = source.unsafe_subscribe(
                state,
                args=args,
            )
            self.result = result
            return state, result
        
    subscription = SinlgeSinkSubscription(result=None)
    
    state = subscribe(
        subscriptions=(subscription,),
        # connections=connections,
        state=state,
    )

    assert subscription.result is not None

    return state, subscription.result



# def subscribe_single_sink_on_trampoline[U](
#     source: FlowableNode[S, U],
#     args: SubscribeArgs[U],
#     state: State,
#     connections: dict[ConnectableFlowableNode[S, U], FlowableNode[S, U]] | None = None,
# ):    
#     def trampoline_task(state=state):
#         _, result = subscribe_single_sink(
#             source=source,
#             args=args,
#             state=state,
#             connections=connections,
#         )
#
#         return result.certificate
#
#     return state.subscription_trampoline.start_loop(
#         task=trampoline_task, 
#         virtual_count=1, 
#         cancellation=None,
#     )


# def subscribe(
#     subscriptions: tuple[Subscription, ...],
#     connections: dict[ConnectableFlowableNode, FlowableNode],
#     state: State,
# ):
#     results = []

#     state = state.copy(connections=connections)

#     # discover sharable nodes
#     for subscription in subscriptions:
#         state = subscription.discover(state)

#     # assign virtual_counts
#     for subscription in subscriptions:
#         state = subscription.assign_virtual_count(state, 1)

#     for connectable in state.discovered_connectables:
#         state = state.connections[connectable].assign_virtual_count(state, 1)

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
