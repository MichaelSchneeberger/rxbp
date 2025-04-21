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
    downstream_observer: Observer
    id: int
    weight: int
    cancellation: CancellationState

    def on_next(
        self,
        trampoline: Trampoline,
        value: None,
    ) -> ContinuationCertificate:
        # print(f"request({self.id})")

        action = RequestTransition(
            child=None,  # type: ignore
            id=self.id,
            weight=self.weight,
        )

        with self.shared.lock:
            action.child = self.shared.action

            match state := action.get_state():
                case AckUpstream():
                    def scheduled_task():
                        return self.shared.upstream_ack_observer.on_next(
                            trampoline, None
                        )
                    
                    certificate = trampoline.schedule(
                        task=scheduled_task, 
                        weight=self.shared.total_weight,
                    )
                    state.certificate, state.acc_certificate = certificate.take(self.weight)

            self.shared.action = ToStateTransition(state)

        match state:
            case AckUpstream() | AwaitOnNext():
                return state.certificate

            case SendItemFromBuffer():
                value = self.shared.get_buffer_item(state.index)

                if state.pop_item:
                    self.shared.pop_buffer(1)

                return self.downstream_observer.on_next(value).subscribe(
                    args=continuationmonad.init_subscribe_args(
                        on_next=self.on_next,
                        weight=self.weight,
                        cancellation=self.cancellation,
                        trampoline=trampoline,
                    )
                )
            
            case TerminatedState(exception=exception):
                return self.downstream_observer.on_error(exception=exception)

            case _:
                raise Exception(f"Unexpected state {state}")
