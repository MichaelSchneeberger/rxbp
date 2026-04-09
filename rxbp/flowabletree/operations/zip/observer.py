from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import override

from contcert.typing import ContCert, Trampoline
from rxsingle.typing import Observer as RxSingleObserver

from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.zip.states import (
    Active,
    StopContinuation,
    Completed,
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
            case Active():
                pass

            case Completed():
                # todo: assure a single certificate
                return self.shared.certs[-1]

            case Error(msg=msg):
                raise Exception(msg)

        n_upstream: deque[tuple[int, RxSingleObserver[None]]] = deque()
        certs: list[ContCert] = []
        for id, observer in self.shared.upstream:
            if id in self.requested_ids:
                cert = observer.on_success(trampoline, None)
                certs.append(cert)

            else:
                n_upstream.append((id, observer))

        self.shared.items = deque()
        self.shared.upstream = n_upstream
        self.shared.certs = certs[1:]

        return certs[0]

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

    # def _on_next(self, state: ZipState, trampoline: Trampoline) -> ContCert:
    #
    #     match state:
    #         # wait for other upstream items
    #         case Active(
    #             requested_ids=requested_ids,
    #             completed_ids=completed_ids,
    #         ):
    #
    #             if requested_ids:
    #
    #                 with self.shared.certs_lock:
    #                     cert = self.shared.certs.pop()
    #                 return cert
    #
    #             else:
    #
    #                 items = dict(self.shared.items)
    #
    #                 # backpressure selected upstream flowables
    #                 self.shared.acc, n_requested_ids = self.shared.zip_func(
    #                     items,
    #                     self.shared.acc,
    #                 )
    #
    #                 complete_downstream = completed_ids & requested_ids
    #
    #                 if complete_downstream:
    #
    #                     transition = OnCompletedTransition(
    #                         child=None,  # pyright: ignore[reportArgumentType]
    #                         id=self.id,
    #                     )
    #
    #                     with self.shared.transition_lock:
    #                         transition.child = self.shared.transition
    #                         self.shared.transition = transition
    #
    #                     cert = self.shared.downstream.on_next_and_complete(
    #                         trampoline=trampoline,
    #                         item=self.shared.acc,
    #                     )
    #
    #                     return cert
    #
    #                 else:
    #                     cert = self.shared.downstream.on_next(
    #                         trampoline=trampoline,
    #                         item=self.shared.acc,
    #                         observer = ZipRxSingleObserver(
    #                             shared=self.shared,
    #                             requested_ids=n_requested_ids,
    #                         ),
    #                     )
    #                     return cert
    #
    #         case StopContinuation(index=index):
    #             cert = self.shared.certs[index]
    #             return cert
    #
    #         case Error(msg):
    #             raise Exception(msg)

    @override
    def on_next(self, trampoline: Trampoline, item: U, observer: RxSingleObserver[None]) -> ContCert:
        # print(f'on_next({value}), id={self.id}')
        self.shared.items.append((self.id, item))
        self.shared.upstream.append((self.id, observer))

        transition = OnNextTransition(
            id=self.id,
            child=None,  # type: ignore  # pyright: ignore[reportArgumentType]
        )

        with self.shared.transition_lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match transition.get_state():
            # wait for other upstream items
            case Active(
                requested_ids=requested_ids,
                completed_ids=completed_ids,
            ):

                if requested_ids:

                    with self.shared.certs_lock:
                        cert = self.shared.certs.pop()
                    return cert

                else:

                    items = dict(self.shared.items)

                    # backpressure selected upstream flowables
                    self.shared.acc, n_requested_ids = self.shared.zip_func(
                        items,
                        self.shared.acc,
                    )

                    complete_downstream = completed_ids & requested_ids

                    if complete_downstream:

                        transition = OnCompletedTransition(
                            child=None,  # pyright: ignore[reportArgumentType]
                            id=self.id,
                        )

                        with self.shared.transition_lock:
                            transition.child = self.shared.transition
                            self.shared.transition = transition

                        match transition.get_state():
                            case StopContinuation(index=index):
                                return self.shared.certs[index]

                            case Completed():
                                cert = self.shared.downstream.on_next_and_complete(
                                    trampoline=trampoline,
                                    item=self.shared.acc,
                                )
                                return cert

                            case Error(msg):
                                raise Exception(msg)

                    else:
                        cert = self.shared.downstream.on_next(
                            trampoline=trampoline,
                            item=self.shared.acc,
                            observer = ZipRxSingleObserver(
                                shared=self.shared,
                                requested_ids=n_requested_ids,
                            ),
                        )
                        return cert

            case StopContinuation(index=index):
                cert = self.shared.certs[index]
                return cert

            case Error(msg):
                raise Exception(msg)

    @override
    def on_next_and_complete(self, trampoline: Trampoline, item: U) -> ContCert:
        # print(f'on_next_and_complete({value}), id={self.id}')
        self.shared.items.append((self.id, item))

        transition = OnNextAndCompleteTransition(
            id=self.id,
            child=None,  # pyright: ignore[reportArgumentType]
        )

        with self.shared.transition_lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match transition.get_state():
            # wait for other upstream items
            case Active(
                requested_ids=requested_ids,
                completed_ids=completed_ids,
            ):

                if requested_ids:

                    with self.shared.certs_lock:
                        cert = self.shared.certs.pop()
                    return cert

                else:

                    items = dict(self.shared.items)

                    # backpressure selected upstream flowables
                    self.shared.acc, n_requested_ids = self.shared.zip_func(
                        items,
                        self.shared.acc,
                    )

                    complete_downstream = completed_ids & n_requested_ids

                    if complete_downstream:

                        transition = OnCompletedTransition(
                            child=None,  # pyright: ignore[reportArgumentType]
                            id=self.id,
                        )

                        with self.shared.transition_lock:
                            transition.child = self.shared.transition
                            self.shared.transition = transition

                        match transition.get_state():
                            case StopContinuation(index=index):
                                return self.shared.certs[index]

                            case Completed():
                                cert = self.shared.downstream.on_next_and_complete(
                                    trampoline=trampoline,
                                    item=self.shared.acc,
                                )
                                return cert

                            case Error(msg):
                                raise Exception(msg)

                    else:
                        cert = self.shared.downstream.on_next(
                            trampoline=trampoline,
                            item=self.shared.acc,
                            observer = ZipRxSingleObserver(
                                shared=self.shared,
                                requested_ids=n_requested_ids,
                            ),
                        )
                        return cert

            case StopContinuation(index=index):
                cert = self.shared.certs[index]
                return cert

            case Error(msg):
                raise Exception(msg)

    @override
    def on_completed(self):
        transition = OnCompletedTransition(
            child=None,  # pyright: ignore[reportArgumentType]
            id=self.id,
        )

        with self.shared.transition_lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match transition.get_state():
            case StopContinuation(index=index):
                return self.shared.certs[index]

            case Completed():
                return self.shared.downstream.on_completed()

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

        match transition.get_state():
            case StopContinuation(index=index):
                return self.shared.certs[index]

            case Completed():
                cert = self.shared.downstream.on_error(exception)
                return cert

            case Error(msg):
                raise Exception(msg)
