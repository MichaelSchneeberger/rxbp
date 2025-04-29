from dataclasses import dataclass
from threading import RLock
from typing import override

from dataclassabc import dataclassabc
from donotation import do

import continuationmonad
from continuationmonad.typing import ContinuationCertificate

from rxbp.cancellable import init_cancellation_state
from rxbp.state import State
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import FlowableNode


@dataclass
class ConnectableObserver[V](Observer):
    lock: RLock
    downstream: Observer
    item_received: bool
    item: V
    certificate: ContinuationCertificate
    is_completed: bool
    exception: Exception | None

    def on_next(self, value: V):
        with self.lock:
            self.item = value
            assert self.item_received is False
            self.item_received = True

        return continuationmonad.from_(None)

    def on_next_and_complete(self, value: V):
        with self.lock:
            self.item = value
            assert self.item_received is False
            self.item_received = True
            self.is_completed = True

        return continuationmonad.from_(self.certificate)

    def on_completed(self):
        return continuationmonad.from_(self.certificate)

    def on_error(self, exception: Exception):
        self.exception = exception
        return continuationmonad.from_(self.certificate)


@dataclassabc(frozen=True)
class ConnectableImpl[V](FlowableNode[V]):
    id: None
    init_item: V

    @override
    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[V],
    ) -> tuple[State, SubscriptionResult]:
        observer = ConnectableObserver(
            lock=state.lock,
            downstream=args.observer,
            item_received=True,
            item=self.init_item,
            is_completed=False,
            exception=None,
            certificate=None,
        )
        
        if self not in state.connections:
            raise AssertionError("Connectable observable is not connected.")

        state = state.copy(
            connectable_observers=state.connectable_observers
            | {state.connections[self]: observer}
        )

        @do()
        def schedule_and_send_next():
            if state.scheduler:
                # schedule sending action on dedicated scheduler
                yield from continuationmonad.schedule_on(state.scheduler)

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
                _ = yield from args.observer.on_next(item)

                yield from continuationmonad.schedule_trampoline()

                return schedule_and_send_next()

            elif is_completed and not item_received:
                return args.observer.on_next_and_complete(item)

            else:
                return args.observer.on_completed()

        cancellable = init_cancellation_state()

        certificate = continuationmonad.fork(
            source=(
                continuationmonad.from_(None)
                .flat_map(lambda _: schedule_and_send_next())
            ),
            on_error=args.observer.on_error,
            scheduler=state.subscription_trampoline,  # ensures scheduling on trampoline
            cancellation=cancellable,
            weight=args.schedule_weight,
        )

        result = SubscriptionResult(
            certificate=certificate,
            cancellable=cancellable,
        )

        return state, result


def init_connectable[V](id, init_item):
    return ConnectableImpl[V](id, init_item)
