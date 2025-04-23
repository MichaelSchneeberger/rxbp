from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from typing import Callable, override

import continuationmonad
from continuationmonad.typing import Scheduler

from rxbp.flowabletree.operations.map import init_map_flowable
from rxbp.state import State, init_state
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import FlowableNode, SingleChildFlowableNode
from rxbp.flowabletree.operations.buffer.flowable import init_buffer
from rxbp.flowabletree.operations.flatmap.flowable import init_flat_map
from rxbp.flowabletree.operations.share.flowable import init_share


class Flowable[V](SingleChildFlowableNode[V, V]):
    @abstractmethod
    def copy(self, /, **changes) -> Flowable: ...

    def buffer(self):
        return self.copy(
            child=init_buffer(
                child=self.child,
            )
        )

    def flat_map(self, func: Callable[[V], FlowableNode]):
        return self.copy(
            child=init_flat_map(
                child=self.child,
                func=func,
            )
        )

    def map[U](self, func: Callable[[V], U]):
        return self.copy(
            child=init_map_flowable(
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

    def run(
        self,
        connections: dict[Flowable, Flowable] | None = None,
    ):
        main_scheduler = continuationmonad.init_main_scheduler()
        subscribe_trampoline = continuationmonad.init_trampoline()

        @dataclass()
        class MainObserver[U](Observer[U]):
            received_items: list[U]
            received_exception: list[Exception | None]

            def on_next(self, value: U):
                # print(f'on_next({value})')
                self.received_items.append(value)
                return continuationmonad.from_(None)

            def on_next_and_complete(self, value: U):
                self.received_items.append(value)
                return continuationmonad.from_(main_scheduler.stop())
                # return main_trampoline.stop()

            def on_completed(self):
                # print('on_complete()')
                return continuationmonad.from_(main_scheduler.stop())
                # return main_trampoline.stop()

            def on_error(self, exception: Exception):
                self.received_exception[0] = exception
                return continuationmonad.from_(main_scheduler.stop())
                # return main_trampoline.stop()

        if connections is None:
            connections = {}

        observer =  MainObserver[V](
            received_items=[],
            received_exception=[None],
        )

        def schedule_task():
            def trampoline_task():

                state = init_state(
                    subscription_trampoline=subscribe_trampoline,
                    scheduler=main_scheduler,
                    connections={c.child: s for c, s in connections.items()},
                )

                result = self.child.subscribe(
                    state=state,
                    args=SubscribeArgs(
                        observer=observer,
                        schedule_weight=1,
                    )
                )

                return result.certificate

            return subscribe_trampoline.run(trampoline_task, weight=1)
        main_scheduler.run(schedule_task)

        if observer.received_exception[0]:
            raise observer.received_exception[0]

        return observer.received_items

    @override
    def unsafe_subscribe(
        self, state: State, args: SubscribeArgs[V]
    ) -> tuple[State, SubscriptionResult]:
        return self.child.unsafe_subscribe(state, args)
