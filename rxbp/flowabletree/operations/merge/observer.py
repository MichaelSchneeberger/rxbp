from __future__ import annotations

from dataclasses import dataclass
from typing import override

from contcert.typing import ContCert

from rxbp.flowabletree.notifications import Notification, OnCompleted, OnError, OnNext, OnNextAndComplete, init_on_completed, init_on_error, init_on_next, init_on_next_and_complete
from rxbp.flowabletree.operations.merge.requestobserver import MergeRequestObserver
from rxbp.flowabletree.operations.merge.states import (
    ActiveState,
    CompletedState,
    ErrorState,
    StopContinuationState,
)
from rxbp.flowabletree.operations.merge.statetransitions import (
    OnCompletedTransition,
    OnErrorTransition,
    OnNextTransition,
    OnNextAndCompleteTransition,
)
from rxbp.flowabletree.operations.merge.sharedmemory import MergeSharedMemory, OnCompletedMergeItem, OnNextMergeItem, OnNextAndCompleteMergeItem
from rxbp.flowabletree.observer import Observer


@dataclass
class MergeObserver[U, E](Observer[U, E]):
    shared: MergeSharedMemory[U, E]
    id: int

    @override
    def notify(
        self,
        notification: Notification[U, E],
    ) -> ContCert:
        match notification:
            case OnNext():
                # print(f"on_next({item})")
                self.shared.items.append(OnNextMergeItem(notification.p_item))
                self.shared.upstream.append((self.id, notification.p_observer))

                transition = OnNextTransition(
                    id=self.id,
                    child=None,  # type: ignore  # pyright: ignore[reportArgumentType]
                )

                with self.shared.transition_lock:
                    transition.child = self.shared.transition
                    self.shared.transition = transition

                match transition.get_state():
                    case ActiveState(
                        received_ids=received_ids,
                    ):
                        if len(received_ids) == 1:
                            # _ = self.shared.items.pop()

                            cert = self.shared.downstream.notify(
                                init_on_next(
                                    item=notification.p_item,
                                    trampoline=notification.p_trampoline,
                                    item_scheduler=notification.p_item_scheduler,
                                    observer=MergeRequestObserver(
                                        shared=self.shared,
                                    ),
                                )
                            )
                            return cert

                        else:
                            with self.shared.certs_lock:
                                cert = self.shared.certs.pop()
                            return cert

                    case StopContinuationState(index=index):
                        cert = self.shared.certs[index]
                        return cert

                    case ErrorState(msg):
                        raise Exception(msg)

            case OnNextAndComplete():
                # print(f"on_next_and_complete({item})")
                self.shared.items.append(OnNextAndCompleteMergeItem(notification.p_item))

                transition = OnNextAndCompleteTransition(
                    id=self.id,
                    child=None,  # type: ignore  # pyright: ignore[reportArgumentType]
                )

                with self.shared.transition_lock:
                    transition.child = self.shared.transition
                    self.shared.transition = transition

                match transition.get_state():
                    case ActiveState(
                        received_ids=received_ids,
                    ):
                        if len(received_ids) == 1:
                            # _ = self.shared.items.pop()
                            cert = self.shared.downstream.notify(
                                init_on_next(
                                    item=notification.p_item,
                                    trampoline=notification.p_trampoline,
                                    item_scheduler=notification.p_item_scheduler,
                                    observer=MergeRequestObserver(
                                        shared=self.shared,
                                    ),
                                )
                            )
                            return cert

                        else:
                            with self.shared.certs_lock:
                                cert = self.shared.certs.pop()
                            return cert

                    case StopContinuationState(index=index):
                        cert = self.shared.certs[index]
                        return cert

                    case CompletedState():
                        cert = self.shared.downstream.notify(
                            init_on_next_and_complete(
                                item=notification.p_item,
                                trampoline=notification.p_trampoline,
                                item_scheduler=notification.p_item_scheduler,
                            )
                        )
                        return cert

                    case ErrorState(msg):
                        raise Exception(msg)

            case OnCompleted():
                self.shared.items.append(OnCompletedMergeItem())

                transition = OnCompletedTransition(
                    child=None,  # pyright: ignore[reportArgumentType]
                    id=self.id,
                )

                with self.shared.transition_lock:
                    transition.child = self.shared.transition
                    self.shared.transition = transition

                match transition.get_state():
                    case ActiveState():
                        with self.shared.certs_lock:
                            cert = self.shared.certs.pop()
                        return cert

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


    # def on_next(self, trampoline: Trampoline, item: U, observer: RxSingleObserver[None]) -> ContCert:
    #     # print(f"on_next({item})")
    #     self.shared.items.append(OnNextMergeItem(item))
    #     self.shared.upstream.append((self.id, observer))
    #
    #     transition = OnNextTransition(
    #         id=self.id,
    #         child=None,  # type: ignore  # pyright: ignore[reportArgumentType]
    #     )
    #
    #     with self.shared.transition_lock:
    #         transition.child = self.shared.transition
    #         self.shared.transition = transition
    #
    #     match transition.get_state():
    #         case ActiveState(
    #             received_ids=received_ids,
    #         ):
    #             if len(received_ids) == 1:
    #                 # _ = self.shared.items.pop()
    #                 cert = self.shared.downstream.on_next(
    #                     trampoline=trampoline,
    #                     item=item,
    #                     observer = MergeRxSingleObserver(
    #                         shared=self.shared,
    #                     ),
    #                 )
    #                 return cert
    #
    #             else:
    #                 with self.shared.certs_lock:
    #                     cert = self.shared.certs.pop()
    #                 return cert
    #
    #         case StopContinuationState(index=index):
    #             cert = self.shared.certs[index]
    #             return cert
    #
    #         case ErrorState(msg):
    #             raise Exception(msg)
    #
    # @override
    # def on_next_and_complete(self, trampoline: Trampoline, item: U) -> ContCert:
    #     # print(f"on_next_and_complete({item})")
    #     self.shared.items.append(OnNextAndCompleteMergeItem(item))
    #
    #     transition = OnNextAndCompleteTransition(
    #         id=self.id,
    #         child=None,  # type: ignore  # pyright: ignore[reportArgumentType]
    #     )
    #
    #     with self.shared.transition_lock:
    #         transition.child = self.shared.transition
    #         self.shared.transition = transition
    #
    #     match transition.get_state():
    #         case ActiveState(
    #             received_ids=received_ids,
    #         ):
    #             if len(received_ids) == 1:
    #                 # _ = self.shared.items.pop()
    #                 cert = self.shared.downstream.on_next(
    #                     trampoline=trampoline,
    #                     item=item,
    #                     observer = MergeRxSingleObserver(
    #                         shared=self.shared,
    #                     ),
    #                 )
    #                 return cert
    #
    #             else:
    #                 with self.shared.certs_lock:
    #                     cert = self.shared.certs.pop()
    #                 return cert
    #
    #         case StopContinuationState(index=index):
    #             cert = self.shared.certs[index]
    #             return cert
    #
    #         case CompletedState():
    #             cert = self.shared.downstream.on_next_and_complete(
    #                 trampoline=trampoline,
    #                 item=item,
    #             )
    #             return cert
    #
    #         case ErrorState(msg):
    #             raise Exception(msg)
    #
    # @override
    # def on_completed(self):
    #     self.shared.items.append(OnCompletedMergeItem())
    #
    #     transition = OnCompletedTransition(
    #         child=None,  # pyright: ignore[reportArgumentType]
    #         id=self.id,
    #     )
    #
    #     with self.shared.transition_lock:
    #         transition.child = self.shared.transition
    #         self.shared.transition = transition
    #
    #     match transition.get_state():
    #         case ActiveState():
    #             with self.shared.certs_lock:
    #                 cert = self.shared.certs.pop()
    #             return cert
    #
    #         case StopContinuationState(index=index):
    #             return self.shared.certs[index]
    #
    #         case CompletedState():
    #             # _ = self.shared.items.pop()
    #             return self.shared.downstream.on_completed()
    #
    #         case ErrorState(msg):
    #             raise Exception(msg)
    #
    # @override
    # def on_error(self, exception: Exception):
    #     transition = OnErrorTransition(
    #         child=None,  # pyright: ignore[reportArgumentType]
    #         id=self.id,
    #     )
    #
    #     with self.shared.transition_lock:
    #         transition.child = self.shared.transition
    #         self.shared.transition = transition
    #
    #     match transition.get_state():
    #         case StopContinuationState(index=index):
    #             return self.shared.certs[index]
    #
    #         case CompletedState():
    #             cert = self.shared.downstream.on_error(exception)
    #             return cert
    #
    #         case ErrorState(msg):
    #             raise Exception(msg)
