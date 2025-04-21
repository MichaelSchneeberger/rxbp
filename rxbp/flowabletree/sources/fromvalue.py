from abc import abstractmethod
from typing import override

from dataclassabc import dataclassabc
from donotation import do

import continuationmonad

from rxbp.cancellable import init_cancellation_state
from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.observeresult import ObserveResult
from rxbp.flowabletree.nodes import FlowableNode


class FromValue[V](FlowableNode[V]):
    @property
    @abstractmethod
    def value(self) -> V: ...

    @override
    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[V],
    ) -> tuple[State, ObserveResult]:
        @do()
        def schedule_and_send_item(value: V):
            if state.scheduler:
                yield from continuationmonad.schedule_on(state.scheduler)

            return args.observer.on_next_and_complete(value)

        cancellable = init_cancellation_state()

        certificate = continuationmonad.fork(
            source=(
                continuationmonad.from_(None)
                .flat_map(lambda _: schedule_and_send_item(self.value))
            ),
            scheduler=state.subscription_trampoline,  # ensures scheduling on trampoline
            cancellation=cancellable,
            weight=args.schedule_weight,
        )

        result = ObserveResult(
            certificate=certificate,
            cancellable=cancellable,
        )

        return state, result


@dataclassabc(frozen=True)
class FromValueImpl[V](FromValue[V]):
    value: V


def init_from_value[V](value: V):
    return FromValueImpl[V](value=value)
