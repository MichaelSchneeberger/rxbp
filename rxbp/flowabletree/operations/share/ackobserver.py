from __future__ import annotations

from dataclasses import dataclass

import continuationmonad
from continuationmonad.typing import (
    ContinuationCertificate,
    Trampoline,
    Observer as CMObserver,
)

from rxbp.cancellable import CancellationState
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.share.states import (
    HasErroredState,
    CancelledKeepAwaitDownstreamState,
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
    weight: int
    cancellation: CancellationState

    def on_success(
        self,
        trampoline: Trampoline,
        weight: int,
        item: None,
    ) -> ContinuationCertificate:
        # print(f"request({item}, id={self.id})")

        transition = RequestTransition(
            child=None,  # type: ignore
            id=self.id,
            weight=weight,
        )

        with self.shared.lock:
            transition.child = self.shared.transition

            match state := transition.get_state():
                case RequestUpstream():
                    # total_weight = sum(self.shared.weight_partition(id) for id in buffer_map)
                    total_weight = sum(w for w in state.weights.values())

                    certificate = self.shared.deferred_handler.resume(
                        trampoline, total_weight, None
                    )

                    # state.certificate, others = certificate.take(weight)

                    requested_certificates = {}
                    for id, weight in state.weights.items():
                        requested_certificates[id], certificate = certificate.take(weight)
                        
                    state.requested_certificates = requested_certificates

            self.shared.transition = ToStateTransition(state)

        match state:
            case RequestUpstream():
                return state.requested_certificates[self.id]
            
            case AwaitOnNext():
                # todo: adapt to new weight
                # either split current certificate, or merge current certificate with borrowed certificate
                return state.certificate

            case CancelledKeepAwaitDownstreamState():
                return state.certificate

            case OnNextFromBuffer():
                item = self.shared.get_buffer_item(state.index)

                if state.pop_item:
                    self.shared.pop_buffer(1)

                def trampoline_task():
                    if state.is_completed:
                        continuation = self.observer.on_next_and_complete(item)
                        observer = continuationmonad.init_anonymous_observer(
                            on_success=lambda t, w, c: c
                        )

                    else:
                        continuation = self.observer.on_next(item)
                        observer=self

                    certificate =  continuation.subscribe(
                        args=continuationmonad.init_subscribe_args(
                            observer=observer,
                            weight=weight,
                            cancellation=self.cancellation,
                            trampoline=trampoline,
                        )
                    )
                    return certificate
                
                return trampoline.schedule(trampoline_task, self.weight, self.cancellation)
            
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
        exception: Exception,
    ) -> ContinuationCertificate:
        return self.observer.on_error(exception).subscribe(
            args=continuationmonad.init_subscribe_args(
                observer=self,
                weight=self.weight,
                cancellation=self.cancellation,
                trampoline=trampoline,
            )
        )