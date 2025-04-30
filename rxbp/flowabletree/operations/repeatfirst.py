from __future__ import annotations

from dataclasses import dataclass

from dataclassabc import dataclassabc

from donotation import do

import continuationmonad
from continuationmonad.typing import DeferredHandler, Trampoline

from rxbp.state import State
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import FlowableNode, SingleChildFlowableNode


@dataclassabc(frozen=True)
class RepeatFirstFlowable[U](SingleChildFlowableNode[U, U]):
    child: FlowableNode[U]

    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[U],
    ) -> tuple[State, SubscriptionResult]:
        @dataclass
        class RepeatFirstObserver(Observer):
            def _fork_continuation(self, item: U):
                def func(trampoline: Trampoline, deferred_handler: DeferredHandler):
                    @do()
                    def schedule_and_send_item():
                        yield from args.observer.on_next(item)

                        yield from continuationmonad.schedule_trampoline()

                        return schedule_and_send_item()

                    return continuationmonad.fork(
                        source=schedule_and_send_item(),
                        on_error=args.observer.on_error,
                        scheduler=trampoline,
                        cancellation=deferred_handler.cancellation,
                        weight=deferred_handler.weight,
                    )
                return func

            def on_next(self, item: U):
                return continuationmonad.defer(self._fork_continuation(item))

            def on_next_and_complete(self, item: U):
                return continuationmonad.defer(self._fork_continuation(item))

            def on_completed(self):
                return args.observer.on_completed()

            def on_error(self, exception: Exception):
                return args.observer.on_error(exception)

        return self.child.unsafe_subscribe(
            state=state,
            args=SubscribeArgs(
                observer=RepeatFirstObserver(),
                schedule_weight=args.schedule_weight,
            ),
        )


def init_repeat_first_flowable[U](
    child: FlowableNode[U],
):
    return RepeatFirstFlowable[U](
        child=child,
    )
