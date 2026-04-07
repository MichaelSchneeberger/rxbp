from __future__ import annotations

from dataclasses import dataclass

from donotation import do

import rxsingle
from rxsingle.typing import (
    ContCert,
    Trampoline,
    DeferredHandler,
)

from rxbp.cancellable import CancellationState
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.share.states import (
    CancelledState,
    HasTerminatedState,
    OnCompletedState,
    OnErrorState,
    OnNext,
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
    virtual_count: int

    @do()
    def on_next(self, item: V):
        # print(f"on_next({item})")

        def on_next_subscription(trampoline: Trampoline, handler: DeferredHandler):
            # print(f"on_next_subscription({item}), virtual_count={handler.virtual_count}")

            self.shared.deferred_handler = handler

            transition = OnNextTransition(
                child=None,  # type: ignore
            )

            with self.shared.lock:
                transition.child = self.shared.transition
                self.shared.transition = transition

            match state := transition.get_state():
                case OnNext():
                    if state.buffer_item:
                        self.shared.buffer.append(item)

                    assert state.send_ids

                    def gen_certificates():
                        for id in state.requested_certificates:
                            if id in state.send_ids:
                                ack_observer = self.ack_observers[id]

                                certificate = ack_observer.observer.on_next(
                                    item
                                ).subscribe(
                                    args=rxsingle.init_subscribe_args(
                                        observer=ack_observer,
                                        # virtual_count=ack_observer.virtual_count,
                                        virtual_count=state.virtual_counts[id],
                                        cancellation=ack_observer.cancellation,
                                        trampoline=trampoline,
                                    )
                                )
                                # todo: merge certificate with requested_certificates

                            else:
                                certificate = state.requested_certificates[id]
                                assert certificate.validated is False

                            yield certificate

                    certificates = tuple(gen_certificates())
                    certificate = (
                        rxsingle.init_composite_continuation_certificate(
                            certificates
                        )
                    )
                    return certificate

                case HasTerminatedState():
                    return ContCert.merge(
                        tuple(state.requested_certificates.values())
                    )

                case _:
                    raise Exception(f"Unexpected state {state}")

        return rxsingle.defer(on_next_subscription)

    @do()
    def on_next_and_complete(self, item: V):
        # print(f"on_next_and_complete({item})")

        transition = OnNextAndCompleteTransition(
            child=None,  # type: ignore
        )

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case OnNext(
                send_ids=send_ids,
            ):
                assert 0 < len(send_ids), f"{len(send_ids)}"

                if state.buffer_item:
                    self.shared.buffer.append(item)

                def gen_certificates():
                    for id in state.requested_certificates:
                        if id in state.send_ids:
                            yield self.ack_observers[id].observer.on_next_and_complete(
                                item
                            )

                        else:
                            certificate = state.requested_certificates[id]
                            assert certificate.validated is False
                            yield rxsingle.from_(certificate)

                return rxsingle.zip(tuple(gen_certificates())).map(
                    lambda c: ContCert.merge(c)
                )

            case HasTerminatedState():
                certificate = ContCert.merge(
                    tuple(state.requested_certificates.values())
                )
                return rxsingle.from_(certificate)

            case _:
                raise Exception(
                    f"Unexpected state {state} when evaluating {transition}."
                )

    def on_completed(self):
        transition = OnCompletedTransition(
            child=None,  # type: ignore
        )

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case OnCompletedState():

                def gen_certificates():
                    for id in state.requested_certificates:
                        if id in state.send_ids:
                            yield self.ack_observers[id].observer.on_completed()

                        else:
                            certificate = state.requested_certificates[id]
                            assert certificate.validated is False
                            yield rxsingle.from_(certificate)

                return rxsingle.zip(tuple(gen_certificates())).map(
                    lambda c: ContCert.merge(c)
                )

                # def gen_certificates():
                #     for id in send_ids:
                #         yield self.ack_observers[id].observer.on_completed()

                # return rxsingle.zip(tuple(gen_certificates())).map(
                #     lambda c: ContCert.merge(
                #         tuple(state.acc_certificate.values()) + c,
                #     )
                # )

            case HasTerminatedState():
                certificate = ContCert.merge(
                    tuple(state.requested_certificates.values())
                )
                return rxsingle.from_(certificate)

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
            case OnErrorState():

                def gen_certificates():
                    for id in state.requested_certificates:
                        if id in state.send_ids:
                            yield self.ack_observers[id].observer.on_error(exception)

                        else:
                            certificate = state.requested_certificates[id]
                            assert certificate.validated is False
                            yield rxsingle.from_(certificate)

                return rxsingle.zip(tuple(gen_certificates())).map(
                    lambda c: ContCert.merge(c)
                )
                # def gen_certificates():
                #     for id in send_ids:
                #         ack_observer = self.ack_observers[id]
                #         yield ack_observer.observer.on_error(exception)

                # return rxsingle.zip(tuple(gen_certificates())).map(
                #     lambda c: ContCert.merge(
                #         tuple(state.acc_certificate.values()) + c
                #     )
                # )

            case HasTerminatedState():
                certificate = ContCert.merge(
                    tuple(state.requested_certificates.values())
                )
                return rxsingle.from_(certificate)

            case _:
                raise Exception(f"Unexpected state {state}.")
