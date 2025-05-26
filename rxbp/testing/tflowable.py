from dataclasses import dataclass
from typing import Callable
import continuationmonad
from continuationmonad.typing import (
    Scheduler,
    ContinuationMonad,
    ContinuationCertificate,
)

from rxbp.cancellable import CancellationState, init_cancellation_state
from rxbp.flowabletree.nodes import FlowableNode
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.state import State


@dataclass(frozen=True)
class TFlowableNode[V](FlowableNode):
    subscribe_func: Callable[
        [Observer, Scheduler | None], ContinuationMonad[ContinuationCertificate]
    ]
    cancellation: CancellationState

    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[V],
    ):
        certificate = continuationmonad.fork(
            source=continuationmonad.from_(None).flat_map(
                lambda _: self.subscribe_func(args.observer, args.scheduler)
            ),
            on_error=args.observer.on_error,
            scheduler=state.subscription_trampoline,  # ensures scheduling on trampoline
            cancellation=None,
            weight=args.weight,
        )

        return state, SubscriptionResult(
            certificate=certificate,
            cancellable=self.cancellation,
        )


def init_test_flowable(
    subscribe_func: Callable[
        [Observer, Scheduler | None], ContinuationMonad[ContinuationCertificate]
    ],
):
    return TFlowableNode(
        subscribe_func=subscribe_func, 
        cancellation=init_cancellation_state()
    )
