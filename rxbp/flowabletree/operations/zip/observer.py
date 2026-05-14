from __future__ import annotations

from dataclasses import dataclass
from typing import override

from contcert.typing import ContCert

from rxbp.flowabletree.notifications import (
    Notification,
    OnNext,
    OnNextAndComplete,
    OnCompleted,
    OnError,
    init_on_next,
    init_on_next_and_complete,
    init_on_completed,
    init_on_error,
)
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.zip.requestobserver import ZipRequestObserver
from rxbp.flowabletree.operations.zip.states import (
    ActiveState,
    StopContinuationState,
    CompletedState,
    ErrorState,
)
from rxbp.flowabletree.operations.zip.statetransitions import (
    OnNextTransition,
    OnNextAndCompleteTransition,
    OnCompletedTransition,
    OnErrorTransition,
)
from rxbp.flowabletree.operations.zip.sharedmemory import ZipSharedMemory


@dataclass
class ZipObserver[U, A, E](Observer[U, E]):
    shared: ZipSharedMemory[U, A, E]
    id: int

    @override
    def notify(
        self,
        notification: Notification[U, E],
    ) -> ContCert:
        match notification:
            case OnNext():
                self.shared.items.append((self.id, notification.p_item))
                self.shared.upstream.append((self.id, notification.p_observer))

                transition = OnNextTransition(
                    id=self.id,
                    child=None,  # type: ignore  # pyright: ignore[reportArgumentType]
                )

                with self.shared.transition_lock:
                    transition.child = self.shared.transition
                    self.shared.transition = transition

                match transition.get_state():
                    # wait for other upstream items
                    case ActiveState(
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
                                    case StopContinuationState(index=index):
                                        return self.shared.certs[index]

                                    case CompletedState():
                                        cert = self.shared.downstream.notify(
                                            init_on_next_and_complete(
                                                item=self.shared.acc,
                                                trampoline=notification.p_trampoline,
                                                item_scheduler=notification.p_item_scheduler,
                                            )
                                        )
                                        return cert

                                    case ErrorState(msg):
                                        raise Exception(msg)

                            else:
                                cert = self.shared.downstream.notify(
                                    init_on_next(
                                        item=self.shared.acc,
                                        trampoline=notification.p_trampoline,
                                        item_scheduler=notification.p_item_scheduler,
                                        observer=ZipRequestObserver(
                                            shared=self.shared,
                                            requested_ids=n_requested_ids,
                                        ),
                                    )
                                )
                                return cert

                    case StopContinuationState(index=index):
                        cert = self.shared.certs[index]
                        return cert

                    case ErrorState(msg):
                        raise Exception(msg)

            case OnNextAndComplete():
                self.shared.items.append((self.id, notification.p_item))

                transition = OnNextAndCompleteTransition(
                    id=self.id,
                    child=None,  # pyright: ignore[reportArgumentType]
                )

                with self.shared.transition_lock:
                    transition.child = self.shared.transition
                    self.shared.transition = transition

                match transition.get_state():
                    # wait for other upstream items
                    case ActiveState(
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
                                    case StopContinuationState(index=index):
                                        return self.shared.certs[index]

                                    case CompletedState():
                                        cert = self.shared.downstream.notify(
                                            init_on_next_and_complete(
                                                item=self.shared.acc,
                                                trampoline=notification.p_trampoline,
                                                item_scheduler=notification.p_item_scheduler,
                                            )
                                        )
                                        return cert

                                    case ErrorState(msg):
                                        raise Exception(msg)

                            else:
                                cert = self.shared.downstream.notify(
                                    init_on_next(
                                        item=self.shared.acc,
                                        trampoline=notification.p_trampoline,
                                        item_scheduler=notification.p_item_scheduler,
                                        observer=ZipRequestObserver(
                                            shared=self.shared,
                                            requested_ids=n_requested_ids,
                                        ),
                                    )
                                )
                                return cert

                    case StopContinuationState(index=index):
                        cert = self.shared.certs[index]
                        return cert

                    case ErrorState(msg):
                        raise Exception(msg)

            case OnCompleted():
                transition = OnCompletedTransition(
                    child=None,  # pyright: ignore[reportArgumentType]
                    id=self.id,
                )

                with self.shared.transition_lock:
                    transition.child = self.shared.transition
                    self.shared.transition = transition

                match transition.get_state():
                    case StopContinuationState(index=index):
                        return self.shared.certs[index]

                    case CompletedState():
                        return self.shared.downstream.notify(
                            init_on_completed(
                                trampoline=notification.p_trampoline,
                                item_scheduler=notification.p_item_scheduler,
                            )
                        )

                    case ErrorState(msg):
                        raise Exception(msg)

            case OnError():
                transition = OnErrorTransition(
                    child=None,  # pyright: ignore[reportArgumentType]
                    id=self.id,
                )

                with self.shared.transition_lock:
                    transition.child = self.shared.transition
                    self.shared.transition = transition

                match transition.get_state():
                    case StopContinuationState(index=index):
                        return self.shared.certs[index]

                    case CompletedState():
                        return self.shared.downstream.notify(
                            init_on_error(
                                exception=notification.p_exception,
                                trampoline=notification.p_trampoline,
                                item_scheduler=notification.p_item_scheduler,
                            )
                        )

                    case ErrorState(msg):
                        raise Exception(msg)
