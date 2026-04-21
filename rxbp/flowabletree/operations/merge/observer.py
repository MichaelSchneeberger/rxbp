from __future__ import annotations

from dataclasses import dataclass
from typing import override


from contcert.typing import ContCert, Trampoline
from rxsingle.typing import Observer as RxSingleObserver

from rxbp.flowabletree.operations.merge.states import (
    Active,
    Completed,
    Error,
    StopContinuation,
)
from rxbp.flowabletree.operations.merge.statetransitions import (
    RequestTransition,
    ToStateTransition,
    OnCompletedTransition,
    OnErrorTransition,
    OnNextTransition,
    OnNextAndCompleteTransition,
)
from rxbp.flowabletree.operations.merge.sharedmemory import MergeSharedMemory, OnCompleted, OnNext, OnNextAndComplete
from rxbp.flowabletree.observer import Observer


@dataclass
class MergeRxSingleObserver[U](RxSingleObserver[None]):
    shared: MergeSharedMemory[U]

    @override
    def on_success(
        self,
        trampoline: Trampoline,
        item: None,
    ):
        # print("Request new")

        transition = RequestTransition(
            child=None,  # pyright: ignore[reportArgumentType]
        )

        with self.shared.transition_lock:
            transition.child = self.shared.transition
            n_state = transition.get_state()
            self.shared.transition = ToStateTransition(
                state=n_state
            )

        match n_state:
            case Active(
                received_ids=received_ids,
            ):
                if not received_ids:
                    match self.shared.items.pop():
                        case OnNext():
                            _, observer = self.shared.upstream.pop()
                            cert = observer.on_success(trampoline, None)
                            return cert
                        case _:
                            with self.shared.certs_lock:
                                cert = self.shared.certs.pop()
                            return cert

                else:
                    match self.shared.items.pop():
                        case OnNext(item=next_item):

                            _, observer = self.shared.upstream.pop()
                            cert = observer.on_success(trampoline, None)
                            with self.shared.certs_lock:
                                self.shared.certs.append(cert)

                            print(f"request: next{item}")
                            cert = self.shared.downstream.on_next(
                                trampoline=trampoline,
                                item=next_item,
                                observer = MergeRxSingleObserver(
                                    shared=self.shared,
                                ),
                            )
                            return cert

                        case OnNextAndComplete(item=next_item):
                            cert = self.shared.downstream.on_next_and_complete(
                                trampoline=trampoline,
                                item=next_item,
                            )
                            return cert

                        case OnCompleted():
                            cert = self.shared.downstream.on_completed()
                            return cert

                        case _:
                            raise Exception()

            case StopContinuation(index=index):
                return self.shared.certs[index]

            case Error(msg=msg):
                raise Exception(msg)

    @override
    def on_error(
        self,
        trampoline: Trampoline,
        exception: Exception,
    ):
        pass

@dataclass
class MergeObserver[U](Observer[U]):
    shared: MergeSharedMemory[U]
    id: int

    @override
    def on_next(self, trampoline: Trampoline, item: U, observer: RxSingleObserver[None]) -> ContCert:
        # print(f"on_next({item})")
        self.shared.items.append(OnNext(item))
        self.shared.upstream.append((self.id, observer))

        transition = OnNextTransition(
            id=self.id,
            child=None,  # type: ignore  # pyright: ignore[reportArgumentType]
        )

        with self.shared.transition_lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match transition.get_state():
            case Active(
                received_ids=received_ids,
            ):
                if len(received_ids) == 1:
                    # _ = self.shared.items.pop()
                    cert = self.shared.downstream.on_next(
                        trampoline=trampoline,
                        item=item,
                        observer = MergeRxSingleObserver(
                            shared=self.shared,
                        ),
                    )
                    return cert

                else:
                    with self.shared.certs_lock:
                        cert = self.shared.certs.pop()
                    return cert

            case StopContinuation(index=index):
                cert = self.shared.certs[index]
                return cert

            case Error(msg):
                raise Exception(msg)

    @override
    def on_next_and_complete(self, trampoline: Trampoline, item: U) -> ContCert:
        # print(f"on_next_and_complete({item})")
        self.shared.items.append(OnNextAndComplete(item))

        transition = OnNextAndCompleteTransition(
            id=self.id,
            child=None,  # type: ignore  # pyright: ignore[reportArgumentType]
        )

        with self.shared.transition_lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match transition.get_state():
            case Active(
                received_ids=received_ids,
            ):
                if len(received_ids) == 1:
                    # _ = self.shared.items.pop()
                    cert = self.shared.downstream.on_next(
                        trampoline=trampoline,
                        item=item,
                        observer = MergeRxSingleObserver(
                            shared=self.shared,
                        ),
                    )
                    return cert

                else:
                    with self.shared.certs_lock:
                        cert = self.shared.certs.pop()
                    return cert

            case StopContinuation(index=index):
                cert = self.shared.certs[index]
                return cert

            case Completed():
                cert = self.shared.downstream.on_next_and_complete(
                    trampoline=trampoline,
                    item=item,
                )
                return cert

            case Error(msg):
                raise Exception(msg)

    @override
    def on_completed(self):
        self.shared.items.append(OnCompleted())

        transition = OnCompletedTransition(
            child=None,  # pyright: ignore[reportArgumentType]
            id=self.id,
        )

        with self.shared.transition_lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match transition.get_state():
            case Active():
                with self.shared.certs_lock:
                    cert = self.shared.certs.pop()
                return cert

            case StopContinuation(index=index):
                return self.shared.certs[index]

            case Completed():
                # _ = self.shared.items.pop()
                return self.shared.downstream.on_completed()

            case Error(msg):
                raise Exception(msg)

    @override
    def on_error(self, exception: Exception):
        transition = OnErrorTransition(
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
