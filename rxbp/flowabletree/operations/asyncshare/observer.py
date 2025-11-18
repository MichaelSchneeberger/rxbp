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
from rxbp.flowabletree.operations.asyncshare.states import (
    ActiveState,
    DownstreamID,
)
from rxbp.flowabletree.operations.asyncshare.statetransitions import (
    OnErrorTransition,
    OnNextTransition,
    OnNextAndCompleteTransition,
    OnCompletedTransition,
)
from rxbp.flowabletree.operations.asyncshare.sharedmemory import ShareSharedMemory
from rxbp.flowabletree.operations.asyncshare.ackobserver import ShareAckObserver


@dataclass
class SharedObserver[V](Observer[V]):
    ack_observers: dict[DownstreamID, ShareAckObserver]
    # cancellations: dict[int, CancellationState]
    shared: ShareSharedMemory
    weight: int

    @do()
    def on_next(self, item: V):
        # print(f"on_next({item})")

        def on_next_subscription(trampoline: Trampoline, handler: DeferredHandler):
            # print(f"on_next_subscription({item}), weight={handler.weight}")

            self.shared.deferred_handler = handler

            transition = OnNextTransition(
                child=None,  # type: ignore
            )

            with self.shared.lock:
                transition.child = self.shared.transition
                self.shared.transition = transition

            match state := transition.get_state():
                case ActiveState(action=ActiveState.OnNext() as action):
                    if action.should_add_to_buffer():
                        self.shared.buffer.append(item)

                    # assert state.send_ids
                    # synced_ids = state.get_synced_ids()

                    def gen_certificates():
                        waiting_downstreams = tuple(state.buffer_map.get_waiting_downstreams())

                        for id in state.buffer_map.get_active_downstream():
                            if id in waiting_downstreams:
                                ack_observer = self.ack_observers[id]
                                # weight = state.weights[id]

                                certificate = ack_observer.observer.on_next(
                                    item
                                ).subscribe(
                                    args=continuationmonad.init_subscribe_args(
                                        observer=ack_observer,
                                        weight=self.weight,
                                        cancellation=ack_observer.cancellation,
                                        trampoline=trampoline,
                                    )
                                )

                            else:
                                certificate = state.get_downstream_certificate(id)
                                assert certificate.validated is False

                            yield certificate

                    certificates = tuple(gen_certificates())
                    certificate = (
                        continuationmonad.init_composite_continuation_certificate(
                            certificates
                        )
                    )
                    return certificate

                case CancelledState():
                    return state.certificate

                case _:
                    raise Exception(f"Unexpected state {state}")

        return continuationmonad.defer(on_next_subscription)

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
            case ActiveState(action=ActiveState.OnNextAndComplete() as action):

                # assert state.send_ids
                # synced_ids = state.get_synced_ids()

                # assert 0 < len(synced_ids), f"{len(synced_ids)}"

                if action.should_add_to_buffer():
                    self.shared.buffer.append(item)

                def gen_certificates():

                    waiting_downstreams = tuple(state.buffer_map.get_waiting_downstreams())

                    for id in state.buffer_map.get_active_downstream():
                        if id in waiting_downstreams:
                            yield self.ack_observers[id].observer.on_next_and_complete(
                                item
                            )

                        else:

                            certificate = state.get_downstream_certificate(id)
                            # certificate = state.requested_certificates[id]
                            assert certificate.validated is False
                            yield continuationmonad.from_(certificate)

                return continuationmonad.zip(tuple(gen_certificates())).map(
                    lambda c: ContinuationCertificate.merge(c)
                )

            case CancelledState():
                return continuationmonad.from_(state.certificate)

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
            case ActiveState():
                # # assert state.send_ids
                # synced_ids = state.get_synced_ids()

                def gen_certificates():
                    for id in state.requested_certificates:
                        if id in state.send_ids:
                            yield self.ack_observers[id].observer.on_completed()

                        else:
                            certificate = state.requested_certificates[id]
                            assert certificate.validated is False
                            yield continuationmonad.from_(certificate)

                return continuationmonad.zip(tuple(gen_certificates())).map(
                    lambda c: ContinuationCertificate.merge(c)
                )

                # def gen_certificates():
                #     for id in send_ids:
                #         yield self.ack_observers[id].observer.on_completed()

                # return continuationmonad.zip(tuple(gen_certificates())).map(
                #     lambda c: ContinuationCertificate.merge(
                #         tuple(state.acc_certificate.values()) + c,
                #     )
                # )

            case CancelledState():
                return continuationmonad.from_(state.certificate)

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
                            yield continuationmonad.from_(certificate)

                return continuationmonad.zip(tuple(gen_certificates())).map(
                    lambda c: ContinuationCertificate.merge(c)
                )
                # def gen_certificates():
                #     for id in send_ids:
                #         ack_observer = self.ack_observers[id]
                #         yield ack_observer.observer.on_error(exception)

                # return continuationmonad.zip(tuple(gen_certificates())).map(
                #     lambda c: ContinuationCertificate.merge(
                #         tuple(state.acc_certificate.values()) + c
                #     )
                # )

            case CancelledState():
                return continuationmonad.from_(state.certificate)

            case _:
                raise Exception(f"Unexpected state {state}.")
