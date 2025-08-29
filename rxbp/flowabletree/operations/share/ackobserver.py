from __future__ import annotations

from dataclasses import dataclass

import continuationmonad
from continuationmonad.typing import (
    ContinuationCertificate,
    Trampoline,
    Observer as CMObserver,
)

from rxbp.logging import rxbp_logger
from rxbp.cancellable import CancellationState
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.share.states import (
    HasErroredState,
    RequestFromCancelledDownstreamState,
    RequestUpstream,
    AwaitOnNext,
    OnNextFromBuffer,
    TerminatedStateMixin,
)
from rxbp.flowabletree.operations.share.statetransitions import (
    RequestTransition,
    ToStateTransition,
)
from rxbp.flowabletree.operations.share.sharedmemory import ShareSharedMemory


@dataclass(frozen=False)
class ShareAckObserver(CMObserver):
    shared: ShareSharedMemory
    observer: Observer
    id: int
    cancellation: CancellationState

    def on_success(
        self,
        trampoline: Trampoline,
        weight: int,
        item: None,
    ) -> ContinuationCertificate:
        rxbp_logger.info(f"ShareAckObserver.on_success({item}, id={self.id})")

        transition = RequestTransition(
            child=None,  # type: ignore
            id=self.id,
            weight=weight,
        )

        with self.shared.lock:
            transition.child = self.shared.transition

            match state := transition.get_state():
                case RequestUpstream():
                    total_weight = sum(w for w in state.weights.values())

                    # request new item from upstream
                    certificate = self.shared.deferred_handler.resume(
                        trampoline, total_weight, None
                    )

                    # Split up the upstream certificate according to the weight of each downstream branch.
                    # Note that at this point all previous requested certificates have been consumed either
                    # through a downstream request or on a upstream on_next call.
                    requested_certificates = {}
                    for id, weight in state.weights.items():
                        requested_certificates[id], certificate = certificate.take(
                            weight
                        )

                    state.requested_certificates = requested_certificates

            self.shared.transition = ToStateTransition(state)

        match state:
            case RequestUpstream():
                return state.requested_certificates[self.id]

            case AwaitOnNext():
                return state.certificate

            case RequestFromCancelledDownstreamState():
                return state.certificate

            case OnNextFromBuffer():
                item = self.shared.get_buffer_item(state.index)

                if state.pop_item:
                    self.shared.pop_buffer(1)

                def trampoline_task():
                    if state.is_completed:
                        continuation = self.observer.on_next_and_complete(item)
                        observer = continuationmonad.init_anonymous_observer[
                            ContinuationCertificate
                        ](on_success=lambda t, w, c: c)

                    else:
                        continuation = self.observer.on_next(item)
                        observer = self

                    certificate = continuation.subscribe(
                        args=continuationmonad.init_subscribe_args(
                            observer=observer,
                            weight=weight,
                            cancellation=self.cancellation,
                            trampoline=trampoline,
                        )
                    )
                    return certificate

                # schedule continuation on the current trampoline to limit the call stack depth
                return trampoline.schedule(trampoline_task, weight, self.cancellation)

            case HasErroredState(exception=exception):
                return self.observer.on_error(exception=exception).subscribe(
                    args=continuationmonad.init_subscribe_args(
                        observer=self,
                        weight=weight,
                        cancellation=self.cancellation,
                        trampoline=trampoline,
                    )
                )

            case TerminatedStateMixin():
                return state.requested_certificates[self.id]

            case _:
                raise Exception(f"Unexpected state {state}")

    def on_error(
        self,
        trampoline: Trampoline,
        weight: int,
        exception: Exception,
    ) -> ContinuationCertificate:
        return self.observer.on_error(exception).subscribe(
            args=continuationmonad.init_subscribe_args(
                observer=self,
                weight=weight,
                cancellation=self.cancellation,
                trampoline=trampoline,
            )
        )

