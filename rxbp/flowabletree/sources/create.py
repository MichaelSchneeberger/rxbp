from dataclasses import dataclass
from typing import Callable, override

import continuationmonad
from continuationmonad.typing import (
    ContinuationMonad,
    ContinuationCertificate,
    Scheduler,
)

from rxbp.cancellable import init_cancellation_state
from rxbp.state import State
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import FlowableNode


@dataclass
class CreateObserver[U](Observer):
    downstream: Observer
    completed_counter: int

    def on_next(self, item: U) -> ContinuationMonad[None]:
        return self.downstream.on_next(item)

    def on_next_and_complete(self, item: U) -> ContinuationMonad[ContinuationCertificate]:
        self.completed_counter += 1
        return self.downstream.on_next_and_complete(item)

    def on_completed(self) -> ContinuationMonad[ContinuationCertificate]:
        self.completed_counter += 1
        return self.downstream.on_completed()

    def on_error(self, exception: Exception) -> ContinuationMonad[ContinuationCertificate]:
        self.completed_counter += 1
        return self.downstream.on_error(exception)



@dataclass(frozen=True)
class Create[V](FlowableNode[V]):
    func: Callable[[Observer, Scheduler], ContinuationMonad[ContinuationCertificate]]

    @override
    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[V],
    ) -> tuple[State, SubscriptionResult]:
        cancellable = init_cancellation_state()

        # observer = CreateObserver(
        #     downstream=args.observer,
        #     is_completed=False,
        # )

        def ensure_completion(certificate):
            if isinstance(certificate, ContinuationCertificate):
                return continuationmonad.from_(certificate)
            
            else:
                return args.observer.on_completed()

        certificate = continuationmonad.fork(
            source=(
                continuationmonad.from_(None)
                .flat_map(lambda _: self.func(args.observer, args.scheduler))
                .flat_map(ensure_completion)
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


def init_create(func: Callable[[Observer, Scheduler], ContinuationMonad[ContinuationCertificate]]):
    return Create(func=func)
