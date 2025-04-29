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
    RequestUpstream,
    AwaitOnNext,
    SendItemFromBuffer,
    HasErroredState,
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
        value: None,
    ) -> ContinuationCertificate:
        # print(f"request({self.id})")

        transition = RequestTransition(
            child=None,  # type: ignore
            id=self.id,
            weight=self.weight,
        )

        with self.shared.lock:
            transition.child = self.shared.transition

            match state := transition.get_state():
                case RequestUpstream():
                    def trampoline_task():
                        return self.shared.deferred_handler.resume(
                            trampoline, None
                        )
                    
                    certificate = trampoline.schedule(
                        task=trampoline_task, 
                        weight=self.shared.total_weight,
                    )

                    state.certificate, state.acc_certificate = certificate.take(self.weight)

            self.shared.transition = ToStateTransition(state)

        match state:
            case RequestUpstream() | AwaitOnNext():
                return state.certificate

            case SendItemFromBuffer():
                value = self.shared.get_buffer_item(state.index)

                if state.pop_item:
                    self.shared.pop_buffer(1)

                return self.observer.on_next(value).subscribe(
                    args=continuationmonad.init_subscribe_args(
                        observer=self,
                        weight=self.weight,
                        cancellation=self.cancellation,
                        trampoline=trampoline,
                    )
                )
            
            case HasErroredState(exception=exception):
                return self.observer.on_error(exception=exception).subscribe(
                    args=continuationmonad.init_subscribe_args(
                        observer=self,
                        weight=self.weight,
                        cancellation=self.cancellation,
                        trampoline=trampoline,
                    )
                )

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