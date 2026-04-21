from __future__ import annotations

from dataclasses import dataclass

import rxsingle
from rxsingle.typing import (
    ContCert,
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
        virtual_count: int,
        item: None,
    ) -> ContCert:
        rxbp_logger.info(f"ShareAckObserver.on_success({item}, id={self.id})")

        transition = RequestTransition(
            child=None,  # type: ignore
            id=self.id,
            virtual_count=virtual_count,
        )

        with self.shared.transition_lock:
            transition.child = self.shared.transition

            match state := transition.get_state():
                case RequestUpstream():
                    total_virtual_count = sum(w for w in state.virtual_counts.values())

                    # request new item from upstream
                    certificate = self.shared.deferred_handler.resume(
                        trampoline, total_virtual_count, None
                    )

                    # Split up the upstream certificate according to the virtual_count of each downstream branch.
                    # Note that at this point all previous requested certificates have been consumed either
                    # through a downstream request or on a upstream on_next call.
                    requested_certificates = {}
                    for id, virtual_count in state.virtual_counts.items():
                        requested_certificates[id], certificate = certificate.take(
                            virtual_count
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
                        observer = rxsingle.init_anonymous_observer[
                            ContCert
                        ](on_success=lambda t, w, c: c)

                    else:
                        continuation = self.observer.on_next(item)
                        observer = self

                    certificate = continuation.subscribe(
                        args=rxsingle.init_subscribe_args(
                            observer=observer,
                            virtual_count=virtual_count,
                            cancellation=self.cancellation,
                            trampoline=trampoline,
                        )
                    )
                    return certificate

                # schedule continuation on the current trampoline to limit the call stack depth
                return trampoline.schedule(trampoline_task, virtual_count, self.cancellation)

            case HasErroredState(exception=exception):
                return self.observer.on_error(exception=exception).subscribe(
                    args=rxsingle.init_subscribe_args(
                        observer=self,
                        virtual_count=virtual_count,
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
        virtual_count: int,
        exception: Exception,
    ) -> ContCert:
        return self.observer.on_error(exception).subscribe(
            args=rxsingle.init_subscribe_args(
                observer=self,
                virtual_count=virtual_count,
                cancellation=self.cancellation,
                trampoline=trampoline,
            )
        )

