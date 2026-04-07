from abc import ABC, abstractmethod
from dataclasses import dataclass
from threading import Lock
from typing import override

from dataclasses import dataclass
from donotation import do

import rxsingle
from contcert.typing import ContCert, contcert

from rxbp.cancellable import init_cancellation_state
from rxbp.state import State
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.subscription import Subscription
from rxbp.flowabletree.flowablenode import FlowableNode


class ConnectableState(ABC):
    @property
    @abstractmethod
    def connections(
            self,
    ) -> dict[ConnectableFlowableNode[S, U], FlowableNode[S, U]]:
        """
        Maps connectable Flowable nodes to its source.

        This dictionary needs to be provided when subscribing to a Flowable.
        """

@dataclass
class ConnectableObserver[V][U](Observer[U]):
    lock: Lock
    downstream: Observer
    item_received: bool
    item: V
    certificate: ContCert
    is_completed: bool
    exception: Exception | None

    def on_next(self, item: V):
        with self.lock:
            self.item = item
            assert self.item_received is False
            self.item_received = True

        return rxsingle.from_(None)

    def on_next_and_complete(self, item: V):
        with self.lock:
            self.item = item
            assert self.item_received is False
            self.item_received = True
            self.is_completed = True

        return rxsingle.from_(self.certificate)

    def on_completed(self):
        return rxsingle.from_(self.certificate)

    def on_error(self, exception: Exception):
        self.exception = exception
        return rxsingle.from_(self.certificate)


@dataclass(frozen=True)
class ConnectableFlowableNode[V](FlowableNode[V]):
    id: None
    init_item: V

    def discover(self, state: State):
        state = state.copy(
            discovered_connectables=state.discovered_connectables + [self]
        )
        return state.connections[self].discover(state)

    # def assign_virtual_count(self, state: State, _):
    #     # state = state.copy(
    #     #     discovered_connectables=state.discovered_connectables + [self]
    #     # )
    #     return state.connections[self].assign_virtual_count(state, 1)

    @override
    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[V],
    ) -> tuple[State, SubscriptionResult]:
        observer = ConnectableObserver(
            lock=Lock(),
            downstream=args.observer,
            item_received=True,
            item=self.init_item,
            is_completed=False,
            exception=None,
            certificate=None,  # type: ignore
        )

        # if self not in state.connections:
        #     raise AssertionError("Connectable observable is not connected.")

        @dataclass
        class ConnectableSubscription(Subscription):
            source: FlowableNode
            observer: ConnectableObserver

            def discover(self, state: State) -> State:
                return self.source.discover(state)

            def assign_virtual_count(self, state: State, virtual_count: int) -> State:
                return self.source.assign_virtual_count(state, virtual_count)

            def apply(self, state: State) -> State:
                state, result = self.source.unsafe_subscribe(
                    state=state,
                    args=args.copy(
                        observer=self.observer,
                        virtual_count=1,
                    ),
                )
                self.observer.certificate = result.certificate
                return state

        subscription = ConnectableSubscription(
            source=state.connections[self],
            observer=observer,
        )

        state = state.copy(
            discovered_subscriptions=state.discovered_subscriptions + [subscription]
        )

        @do()
        def schedule_and_send_next():
            # if args.scheduler:
            #     # schedule sending action on dedicated scheduler
            #     yield from rxsingle.schedule_on(state.scheduler)

            with observer.lock:
                item_received = observer.item_received
                observer.item_received = False
                item = observer.item
                is_completed = observer.is_completed
                exception = observer.exception

            if exception is not None:
                return args.observer.on_error(exception)

            elif not is_completed:
                assert item_received is True

                # receive acknowledgment
                _ = yield from args.observer.on_next(trampoline, item, observer)

                yield from rxsingle.schedule_trampoline()

                # help pyright to infer correct type
                continuation: RxSingle[ContCert] = (
                    schedule_and_send_next()
                )
                return continuation

            elif item_received:
                return args.observer.on_next_and_complete(item)

            else:
                return args.observer.on_completed()

        cancellable = init_cancellation_state()

        certificate = rxsingle.fork(
            source=(
                rxsingle.from_(None).flat_map(
                    lambda _: schedule_and_send_next()
                )
            ),
            on_error=args.observer.on_error,
            scheduler=state.subscription_trampoline,  # ensures scheduling on trampoline
            cancellation=cancellable,
            virtual_count=args.virtual_count,
        )

        result = SubscriptionResult(
            certificate=certificate,
            cancellable=cancellable,
        )

        return state, result


def init_connectable[V](id, init_item):
    return ConnectableFlowableNode[V](id, init_item)
