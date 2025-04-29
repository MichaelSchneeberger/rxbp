from dataclasses import dataclass
from typing import Callable
import continuationmonad
from continuationmonad.typing import (
    Scheduler,
    ContinuationMonad,
    DeferredHandler,
    ContinuationCertificate,
)

from rxbp.cancellable import init_cancellation_state
from rxbp.flowabletree.nodes import FlowableNode
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.state import State


@dataclass
class TFlowableNode[V](FlowableNode):
    subscribe_func: Callable[[Observer], ContinuationMonad[ContinuationCertificate]]

    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[V],
    ):
        certificate = continuationmonad.fork(
            source=self.subscribe_func(args.observer),
            on_error=args.observer.on_error,
            scheduler=state.subscription_trampoline,  # ensures scheduling on trampoline
            cancellation=None,
            weight=args.schedule_weight,
        )

        return state, SubscriptionResult(
            certificate=certificate,
            cancellable=init_cancellation_state(),
        )


def init_test_flowable(
    subscribe_func: Callable[[Observer], ContinuationMonad[ContinuationCertificate]],
):
    return TFlowableNode(
        subscribe_func=subscribe_func,
    )
