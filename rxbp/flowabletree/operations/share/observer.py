from __future__ import annotations

from dataclasses import dataclass

from donotation import do

import continuationmonad
from continuationmonad.typing import (
    ContinuationCertificate,
    Trampoline,
    DeferredHandler,
)

from rxbp.cancellable import CancellationState
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.share.states import (
    OnCompletedState,
    OnErrorState,
    SendItem,
)
from rxbp.flowabletree.operations.share.statetransitions import (
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

        def on_next_subscription(
            trampoline: Trampoline, handler: DeferredHandler
        ):
            # print(f"on_next_subscription({value}), weight={deferred_observer.weight}")

            self.shared.deferred_handler = handler

            transition = OnNextTransition(
                child=None,  # type: ignore
            )

            with self.shared.lock:
                transition.child = self.shared.transition
                self.shared.transition = transition

            match state := transition.get_state():
                case SendItem():
                    if state.buffer_item:
                        self.shared.buffer.append(value)

                    def gen_certificates():
                        for id in state.send_ids:
                            ack_observer = self.ack_observers[id]

                            certificate = ack_observer.observer.on_next(
                                value
                            ).subscribe(
                                args=continuationmonad.init_subscribe_args(
                                    observer=ack_observer,
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

        return continuationmonad.defer(on_next_subscription)

    @do()
    def on_next_and_complete(self, value: V):
        # print(f"on_next_and_complete({value})")

        transition = OnNextAndCompleteTransition(
            child=None,  # type: ignore
        )

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case SendItem(
                send_ids=send_ids,
                acc_certificate=acc_certificate,
            ):
                assert 0 < len(send_ids), f"{len(send_ids)}"

                def gen_certificates():
                    for id in send_ids:
                        ack_observer = self.ack_observers[id]
                        yield ack_observer.observer.on_next_and_complete(
                            value
                        )

                return continuationmonad.zip(tuple(gen_certificates())).map(
                    lambda c: ContinuationCertificate.merge((acc_certificate,) + c)
                )

            case _:
                raise Exception(f"Unexpected state {state}")

    def on_completed(self):
        transition = OnCompletedTransition(
            child=None,  # type: ignore
        )

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case OnCompletedState(
                send_ids=send_ids,
                acc_certificate=acc_certificate,
            ):
                def gen_certificates():
                    for id in send_ids:
                        ack_observer = self.ack_observers[id]
                        yield ack_observer.observer.on_completed()

                return continuationmonad.zip(tuple(gen_certificates())).map(
                    lambda c: ContinuationCertificate.merge((acc_certificate,) + c)
                )

            case _:
                raise Exception(f"Unexpected state {state}.")

    def on_error(self, exception: Exception):
        transition = OnErrorTransition(
            exception=exception,
            child=None,  # type: ignore
        )

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case OnErrorState(
                send_ids=send_ids,
                acc_certificate=acc_certificate,
            ):

                def gen_certificates():
                    for id in send_ids:
                        ack_observer = self.ack_observers[id]
                        yield ack_observer.observer.on_error(exception)

                return continuationmonad.zip(tuple(gen_certificates())).map(
                    lambda c: ContinuationCertificate.merge((acc_certificate,) + c)
                )

            case _:
                raise Exception(f"Unexpected state {state}.")
