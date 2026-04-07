from __future__ import annotations

from dataclasses import dataclass
from typing import override

from contcert.typing import ContCert, Trampoline
from rxsingle.typing import Observer as RxSingleObserver

from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.zip.states import (
    AwaitUpstream,
    StopContinuation,
    CancelledUpstreamPending,
    Completed,
    AwaitRequest,
    Terminated,
    Error,
)
from rxbp.flowabletree.operations.zip.statetransitions import (
    RequestTransition,
    OnNextTransition,
    OnNextAndCompleteTransition,
    OnCompletedTransition,
    ToStateTransition,
)
from rxbp.flowabletree.operations.zip.sharedmemory import ZipSharedMemory


@dataclass
class ZipRxSingleObserver[A, U](RxSingleObserver[None]):
    shared: ZipSharedMemory[A, U]
    requested_ids: set[int]

    @override
    def on_success(
        self,
        trampoline: Trampoline,
        item: None,
    ):
        def gen_certificates():
            for _, o in self.shared.upstream:
                yield o.on_success(trampoline, None)

        cert, *self.shared.certs = tuple(gen_certificates())

        transition = RequestTransition(
            requested_ids=self.requested_ids,
            child=None,  # pyright: ignore[reportArgumentType]
        )

        with self.shared.transition_lock:
            transition.child = self.shared.transition
            n_state = transition.get_state()
            self.shared.transition = ToStateTransition(
                state=n_state
            )

        match n_state:
            case Terminated():
                return self.shared.certs[-1]

        return cert

    @override
    def on_error(
        self,
        trampoline: Trampoline,
        exception: Exception,
    ):
        pass


@dataclass
class ZipObserver[U, V](Observer[U]):
    shared: ZipSharedMemory[U, V]
    id: int

    @override
    def on_next(self, trampoline: Trampoline, item: U, observer: RxSingleObserver[None]) -> ContCert:
        # print(f'on_next({value}), id={self.id}')
        self.shared.items.append((self.id, item))

        transition = OnNextTransition(
            id=self.id,
            child=None,  # type: ignore  # pyright: ignore[reportArgumentType]
        )

        with self.shared.transition_lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            # wait for other upstream items
            case AwaitUpstream():
                self.shared.upstream.append((self.id, observer))
                with self.shared.certs_lock:
                    cert = self.shared.certs.pop()
                return cert

            # all upstream items received
            case AwaitRequest(completed_ids=completed_ids):
                items = dict(self.shared.items)

                # backpressure selected upstream flowables
                self.shared.acc, requested_ids = self.shared.zip_func(
                    items,
                    self.shared.acc,
                )

                complete_downstream = completed_ids & requested_ids

                if complete_downstream:
                    cert = self.shared.downstream.on_next_and_complete(
                        trampoline=trampoline,
                        item=self.shared.acc,
                    )
                    return cert

                else:
                    cert = self.shared.downstream.on_next(
                        trampoline=trampoline,
                        item=self.shared.acc,
                        observer = ZipRxSingleObserver(
                            shared=self.shared,
                        ),
                    )
                    return cert

            # case OnNextAndComplete(
            #     # values=values,
            # ):
            #     _, _, on_next_items = self.shared.zip_func(self.shared.acc, values)
            #     return self.shared.downstream.on_next_and_complete(on_next_items)

            # case Completed():
            #     cert = self.shared.certs[-1]
            #     return cert

            case StopContinuation(index=index):
                self.shared.assigned_certs[index]

            case Error(msg):
                raise Exception(msg)

    @override
    def on_next_and_complete(self, trampoline: Trampoline, item: U) -> ContCert:
        # print(f'on_next_and_complete({value}), id={self.id}')

        transition = OnNextAndCompleteTransition(
            id=self.id,
            child=None,  # pyright: ignore[reportArgumentType]
        )

        with self.shared.transition_lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            # wait for other upstream items
            case AwaitUpstream():
                with self.shared.certs_lock:
                    cert = self.shared.certs.pop()
                return cert

            case AwaitRequest(
                completed_ids=completed_ids,
            ):
                items = dict(self.shared.items)

                # backpressure selected upstream flowables
                self.shared.acc, requested_ids = self.shared.zip_func(
                    items,
                    self.shared.acc,
                )

                complete_downstream = completed_ids & requested_ids

                if complete_downstream:
                    cert = self.shared.downstream.on_next_and_complete(
                        trampoline=trampoline,
                        item=self.shared.acc,
                    )
                    return cert

                else:
                    cert = self.shared.downstream.on_next(
                        trampoline=trampoline,
                        item=self.shared.acc,
                        observer = ZipRxSingleObserver(
                            shared=self.shared,
                            requested_ids=requested_ids,
                        ),
                    )
                    return cert

            case StopContinuation(index=index):
                self.shared.assigned_certs[index]

            case Error(msg):
                raise Exception(msg)

    def on_completed(self):
        transition = OnCompletedTransition(
            child=None,  # pyright: ignore[reportArgumentType]
            id=self.id,
        )

        with self.shared.transition_lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case Completed():

                # if complete_downstream:
                return self.shared.downstream.on_completed()

                # else:
                #     for id, certificate in certificates.items():
                        # self.shared.cancellables[id].cancel(certificate)

            case StopContinuation(index=index):
                self.shared.assigned_certs[index]

            case Error(msg):
                raise Exception(msg)

    @override
    def on_error(self, exception: Exception):
        transition = OnCompletedTransition(
            child=None,  # pyright: ignore[reportArgumentType]
            id=self.id,
        )

        with self.shared.transition_lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case Completed():

                # if complete_downstream:
                cert = self.shared.downstream.on_error(exception)
                return cert

                # else:
                #     for id, certificate in certificates.items():
                        # self.shared.cancellables[id].cancel(certificate)

            case StopContinuation(index=index):
                return self.shared.assigned_certs[index]

            case Error(msg):
                raise Exception(msg)
