from dataclasses import dataclass
from typing import Callable, override

import continuationmonad
from continuationmonad.typing import (
    ContinuationMonad,
    ContinuationCertificate,
    Scheduler,
)

from rxbp.cancellable import init_cancellation_state
from rxbp.flowabletree.observer import Observer
from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import FlowableNode


@dataclass(frozen=True)
class Create[V](FlowableNode[V]):
    func: Callable[[Observer, Scheduler | None], ContinuationMonad[ContinuationCertificate]]

    @override
    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[V],
    ) -> tuple[State, SubscriptionResult]:
        cancellable = init_cancellation_state()

        certificate = continuationmonad.fork(
            source=continuationmonad.from_(None).flat_map(
                lambda _: self.func(args.observer, state.scheduler)
            ),
            on_error=args.observer.on_error,
            scheduler=state.subscription_trampoline,  # ensures scheduling on trampoline
            cancellation=cancellable,
            weight=args.weight,
        )

        return state, SubscriptionResult(
            certificate=certificate,
            cancellable=init_cancellation_state(),
        )


def init_create(func: Callable[[Observer, Scheduler | None], ContinuationMonad[ContinuationCertificate]]):
    return Create(func=func)
