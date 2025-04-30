from dataclasses import dataclass
from typing import Callable, override

from dataclassabc import dataclassabc
from donotation import do

import continuationmonad
from continuationmonad.typing import ContinuationMonad, ContinuationCertificate

from rxbp.cancellable import init_cancellation_state
from rxbp.flowabletree.observer import Observer
from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import FlowableNode


@dataclass(frozen=True)
class Create[V](FlowableNode[V]):
    func: Callable[[Observer], ContinuationMonad[ContinuationCertificate]]

    @override
    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[V],
    ) -> tuple[State, SubscriptionResult]:
        if state.scheduler:
            source = (
                continuationmonad.schedule_on(state.scheduler)
                .flat_map(lambda _: self.func(args.observer))
            )
        
        else:
            source = self.func(args.observer)

        cancellable = init_cancellation_state()

        certificate = continuationmonad.fork(
            source=source,
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


def init_create(func: Callable[[Observer], ContinuationMonad[ContinuationCertificate]]):
    return Create(func=func)
