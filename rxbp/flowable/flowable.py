from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from typing import Callable, override

import continuationmonad
from continuationmonad.typing import Scheduler, MainScheduler

from rxbp.flowabletree.operations.reduce import init_reduce_flowable
from rxbp.flowabletree.operations.repeatfirst import init_repeat_first_flowable
from rxbp.flowabletree.operations.tolist import init_to_list_flowable
from rxbp.state import State, init_state
from rxbp.flowabletree.operations.accumulate import init_accumulate_flowable
from rxbp.flowabletree.operations.defaultifempty import init_default_if_empty_flowable
from rxbp.flowabletree.operations.doaction import init_do_action_flowable
from rxbp.flowabletree.operations.filter import init_filter_flowable
from rxbp.flowabletree.operations.map import init_map_flowable
from rxbp.flowabletree.operations.skipwhile import init_skip_while_flowable
from rxbp.flowabletree.operations.takewhile import init_take_while_flowable
from rxbp.flowabletree.operations.zip.flowable import init_zip
from rxbp.flowabletree.subscribe import subscribe
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import FlowableNode, SingleChildFlowableNode
from rxbp.flowabletree.operations.buffer.flowable import init_buffer
from rxbp.flowabletree.operations.flatmap.flowable import init_flat_map
from rxbp.flowabletree.operations.share.flowable import init_share
from rxbp.flowabletree.from_ import count as _count


class Flowable[U](SingleChildFlowableNode[U, U]):
    def accumulate(self, func, init):
        return self.copy(child=init_accumulate_flowable(self.child, func, init))

    @abstractmethod
    def copy(self, /, **changes) -> Flowable: ...

    def buffer(self):
        return self.copy(
            child=init_buffer(
                child=self.child,
            )
        )

    def default_if_empty[V](self, value: V):
        return self.copy(
            child=init_default_if_empty_flowable(
                child=self.child,
                value=value,
            )
        )

    def do_action(
        self, on_next=None, on_next_and_completed=None, on_completed=None, on_error=None
    ):
        return self.copy(
            child=init_do_action_flowable(
                self.child,
                on_next,
                on_next_and_completed,
                on_completed,
                on_error,
            )
        )

    def filter(self, predicate: Callable[[U], bool]):
        return self.copy(
            child=init_filter_flowable(
                child=self.child,
                predicate=predicate,
            )
        )

    def first(self):
        return self.take(count=1)

    def flat_map(self, func: Callable[[U], FlowableNode]):
        return self.copy(
            child=init_flat_map(
                child=self.child,
                func=func,
            )
        )

    def last(self):
        return self.to_list(size=1).map(lambda v: v[0])
        # return self.copy(child=init_last_flowable(self.child))

    def map[V](self, func: Callable[[U], V]):
        return self.copy(
            child=init_map_flowable(
                child=self.child,
                func=func,
            )
        )

    def reduce(self, func):
        return self.copy(child=init_reduce_flowable(self.child, func))

    def repeat_first(self):
        return self.copy(child=init_repeat_first_flowable(child=self.child))

    def share(self):
        return self.copy(
            child=init_share(
                child=self.child,
            )
        )

    def skip(self, count: int):
        return (
            self.copy(child=init_zip((self, _count())))
            .skip_while(lambda v: v[1] < count)
            .map(lambda v: v[0])
        )

    def skip_while(self, predicate):
        return self.copy(
            child=init_skip_while_flowable(child=self.child, predicate=predicate)
        )

    def take(self, count: int):
        return (
            self.zip_with_index().take_while(lambda v: v[1] < count).map(lambda v: v[0])
        )

    def take_while(self, predicate):
        return self.copy(
            child=init_take_while_flowable(child=self.child, predicate=predicate)
        )

    def to_list(self, size: int | None = None):
        return self.copy(child=init_to_list_flowable(child=self.child, size=size))

    def run(
        self,
        scheduler: MainScheduler | None = None,
        connections: dict[Flowable, Flowable] | None = None,
    ):
        if scheduler is None:
            scheduler = continuationmonad.init_main_scheduler()

        subscribe_trampoline = continuationmonad.init_trampoline()

        @dataclass()
        class MainObserver(Observer[U]):
            received_items: list[U]
            received_exception: list[Exception | None]

            def on_next(self, value: U):
                # print(f'on_next({value})')
                self.received_items.append(value)
                return continuationmonad.from_(None)

            def on_next_and_complete(self, value: U):
                self.received_items.append(value)
                return continuationmonad.from_(scheduler.stop())
                # return main_trampoline.stop()

            def on_completed(self):
                # print('on_complete()')
                return continuationmonad.from_(scheduler.stop())
                # return main_trampoline.stop()

            def on_error(self, exception: Exception):
                self.received_exception[0] = exception
                return continuationmonad.from_(scheduler.stop())
                # return main_trampoline.stop()

        if connections is None:
            connections = {}

        observer = MainObserver(
            received_items=[],
            received_exception=[None],
        )

        def schedule_task():
            def trampoline_task():
                state = init_state(
                    subscription_trampoline=subscribe_trampoline,
                    scheduler=scheduler,
                    connections={c.child: s for c, s in connections.items()},
                )

                results = subscribe(
                    sources=(self.child,),
                    sinks=(
                        SubscribeArgs(
                            observer=observer,
                            schedule_weight=1,
                        ),
                    ),
                    state=state,
                )

                return results[0].certificate

            return subscribe_trampoline.run(trampoline_task, weight=1)

        scheduler.run(schedule_task)

        if observer.received_exception[0]:
            raise observer.received_exception[0]

        return observer.received_items

    def zip_with_index(self):
        return self.copy(child=init_zip((self, _count())))

    def zip(self, others: tuple[Flowable, ...]):
        return self.copy(
            child=init_zip(children=(self,) + others),
        )

    @override
    def unsafe_subscribe(
        self, state: State, args: SubscribeArgs[U]
    ) -> tuple[State, SubscriptionResult]:
        return self.child.unsafe_subscribe(state, args)
