from __future__ import annotations

from dataclasses import dataclass

from donotation import do

import continuationmonad
from continuationmonad.typing import (
    ContinuationCertificate,
    Trampoline,
    DeferredObserver,
)

from rxbp.cancellable import CancellationState
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.share.states import (
    CompleteState,
    ErrorState,
    SendItem,
)
from rxbp.flowabletree.operations.share.transitions import (
    OnErrorTransition,
    OnNextTransition,
    OnNextAndCompleteTransition,
    OnCompletedTransition,
)
from rxbp.flowabletree.operations.share.sharedmemory import ShareSharedMemory
from rxbp.flowabletree.operations.share.ackobserver import ShareAckObserver


@dataclass
class SharedObserver[V](Observer[V]):
    ack_observers: dict[int, ShareAckObserver]
    cancellations: dict[int, CancellationState]
    shared: ShareSharedMemory
    weight: int

    @do()
    def on_next(self, value: V):
        # print(f"on_next({value})")

        def on_next_ackowledgment(
            trampoline: Trampoline, deferred_observer: DeferredObserver
        ):
            # print(f"on_next_subscription({value}), weight={deferred_observer.weight}")

            self.shared.upstream_ack_observer = deferred_observer

            action = OnNextTransition(
                child=None,  # type: ignore
            )

            with self.shared.lock:
                action.child = self.shared.action
                self.shared.action = action

            match state := action.get_state():
                case SendItem():
                    if state.buffer_item:
                        self.shared.buffer.append(value)

                    def gen_certificates():
                        for id in state.send_ids:
                            ack_observer = self.ack_observers[id]

                            certificate = ack_observer.downstream_observer.on_next(
                                value
                            ).subscribe(
                                args=continuationmonad.init_subscribe_args(
                                    on_next=ack_observer.on_next,
                                    weight=ack_observer.weight,
                                    cancellation=ack_observer.cancellation,
                                    trampoline=trampoline,
                                )
                            )

                            yield certificate

                    certificate = ContinuationCertificate.merge(
                        (state.acc_certificate,) + tuple(gen_certificates())
                    )

                    return certificate

                case _:
                    raise Exception(f"Unexpected state {state}")

        return continuationmonad.defer(on_next_ackowledgment)

    @do()
    def on_next_and_complete(self, value: V):
        # print(f"on_next_and_complete({value})")

        action = OnNextAndCompleteTransition(
            child=None,  # type: ignore
        )

        with self.shared.lock:
            action.child = self.shared.action
            self.shared.action = action

        match state := action.get_state():
            case SendItem(
                send_ids=send_ids,
                acc_certificate=acc_certificate,
            ):
                assert 0 < len(send_ids), f"{len(send_ids)}"

                def gen_certificates():
                    for id in send_ids:
                        ack_observer = self.ack_observers[id]
                        yield ack_observer.downstream_observer.on_next_and_complete(
                            value
                        )

                def print_(v):
                    print(v)
                    return v
                return continuationmonad.zip(tuple(gen_certificates())).map(
                    lambda c: ContinuationCertificate.merge((acc_certificate,) + print_(c))
                )

            case _:
                raise Exception(f"Unexpected state {state}")

    def on_completed(self):
        action = OnCompletedTransition(
            child=None,  # type: ignore
        )

        with self.shared.lock:
            action.child = self.shared.action
            self.shared.action = action

        match state := action.get_state():
            case CompleteState(
                send_ids=send_ids,
                acc_certificate=acc_certificate,
            ):
                def gen_certificates():
                    for id in send_ids:
                        ack_observer = self.ack_observers[id]
                        yield ack_observer.downstream_observer.on_completed()

                return continuationmonad.zip(tuple(gen_certificates())).map(
                    lambda c: ContinuationCertificate.merge((acc_certificate,) + c)
                )

            case _:
                raise Exception(f"Unexpected state {state}.")

    def on_error(self, exception: Exception):
        action = OnErrorTransition(
            exception=exception,
            child=None,  # type: ignore
        )

        with self.shared.lock:
            action.child = self.shared.action
            self.shared.action = action

        match state := action.get_state():
            case ErrorState(
                send_ids=send_ids,
                acc_certificate=acc_certificate,
            ):

                def gen_certificates():
                    for id in send_ids:
                        ack_observer = self.ack_observers[id]
                        yield ack_observer.downstream_observer.on_error(exception)

                return continuationmonad.zip(tuple(gen_certificates())).map(
                    lambda c: ContinuationCertificate.merge((acc_certificate,) + c)
                )

            case _:
                raise Exception(f"Unexpected state {state}.")
