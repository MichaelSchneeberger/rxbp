from __future__ import annotations

from dataclasses import dataclass

import continuationmonad
from continuationmonad.typing import (
    ContinuationCertificate,
    Trampoline,
)

from rxbp.cancellable import CancellationState
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.share.states import (
    AckUpstream,
    AwaitOnNext,
    SendItemFromBuffer,
    TerminatedState,
)
from rxbp.flowabletree.operations.share.transitions import (
    RequestTransition,
    ToStateTransition,
)
from rxbp.flowabletree.operations.share.sharedmemory import ShareSharedMemory


@dataclass(frozen=False)
class ShareAckObserver:
    shared: ShareSharedMemory
    observer: Observer
    id: int
    weight: int
    cancellation: CancellationState

    def on_next(
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
                case AckUpstream():
                    def trampoline_task():
                        return self.shared.deferred_observer.on_next(
                            trampoline, None
                        )
                    
                    certificate = trampoline.schedule(
                        task=trampoline_task, 
                        weight=self.shared.total_weight,
                    )

                    state.certificate, state.acc_certificate = certificate.take(self.weight)

            self.shared.transition = ToStateTransition(state)

        match state:
            case AckUpstream() | AwaitOnNext():
                return state.certificate

            case SendItemFromBuffer():
                value = self.shared.get_buffer_item(state.index)

                if state.pop_item:
                    self.shared.pop_buffer(1)

                return self.observer.on_next(value).subscribe(
                    args=continuationmonad.init_subscribe_args(
                        on_next=self.on_next,
                        weight=self.weight,
                        cancellation=self.cancellation,
                        trampoline=trampoline,
                    )
                )
            
            case TerminatedState(exception=exception):
                return self.observer.on_error(exception=exception)

            case _:
                raise Exception(f"Unexpected state {state}")
