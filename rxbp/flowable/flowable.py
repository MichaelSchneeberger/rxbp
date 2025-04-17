from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from typing import Callable, override

import continuationmonad
from continuationmonad.typing import Scheduler

from rxbp.state import State, init_state
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.observeresult import ObserveResult
from rxbp.flowabletree.nodes import FlowableNode, SingleChildFlowableNode
from rxbp.flowabletree.operations.flatmap.flowable import init_flat_map
from rxbp.flowabletree.operations.share.flowable import init_share



class Flowable[V](SingleChildFlowableNode[V, V]):

    @abstractmethod
    def copy(self, /, **changes) -> Flowable: ...

    def flat_map(self, func: Callable[[V], FlowableNode]):
        return self.copy(
            child=init_flat_map(
                child=self.child,
                func=func,
            )
        )

    def share(self):
        return self.copy(
            child=init_share(
                child=self.child,
            )
        )

    def run(self, scheduler: Scheduler | None = None):
        main_trampoline = continuationmonad.init_main_trampoline()

        @dataclass()
        class MainObserver[U](Observer[U]):
            received_items: list[U]
            received_exception: list[Exception | None]

            def on_next(self, value: U):
                self.received_items.append(value)
                return continuationmonad.from_(None)

            def on_next_and_complete(self, value: U):
                self.received_items.append(value)
                return continuationmonad.from_(main_trampoline.stop())
                # return main_trampoline.stop()

            def on_completed(self):
                return continuationmonad.from_(main_trampoline.stop())
                # return main_trampoline.stop()

            def on_error(self, exception: Exception):
                self.received_exception[0] = exception
                return continuationmonad.from_(main_trampoline.stop())
                # return main_trampoline.stop()

        received_items: list[V] = []
        received_exception: list[Exception | None] = [None]

        subscriber_count = {}
        shared_weights = {}

        self.child.discover(subscriber_count)
        self.child.assign_weights(1, shared_weights, subscriber_count)

        state = init_state(
            subscription_trampoline=main_trampoline,
            scheduler=scheduler,
            shared_weights=shared_weights,
        )

        sink = MainObserver[V](
            received_items=received_items,
            received_exception=received_exception,
        )

        args = SubscribeArgs(
            observer=sink,
            schedule_weight=1,
        )

        def main_action():
            _, result = self.child.unsafe_subscribe(state, args)
            return result.certificate

        main_trampoline.run(main_action)

        if received_exception[0]:
            raise received_exception[0]

        return received_items
    
    @override
    def unsafe_subscribe(
        self, state: State, args: SubscribeArgs[V]
    ) -> tuple[State, ObserveResult]:
        return self.child.unsafe_subscribe(state, args)
