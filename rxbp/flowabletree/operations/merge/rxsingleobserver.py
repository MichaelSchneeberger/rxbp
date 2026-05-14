from __future__ import annotations

from dataclasses import dataclass
from typing import Never, override

from rxbp.flowabletree.notifications import (
    init_on_completed,
    init_on_next,
    init_on_next_and_complete,
)
from rxbp.flowabletree.operations.merge.sharedmemory import (
    MergeSharedMemory,
    OnCompletedMergeItem,
    OnNextAndCompleteMergeItem,
    OnNextMergeItem,
)
import rxsingle
from rxsingle.typing import (
    Notification,
    Observer as RxSingleObserver,
    OnError,
    OnSuccess,
)

from rxbp.flowabletree.operations.merge.states import (
    ActiveState,
    ErrorState,
    StopContinuationState,
)
from rxbp.flowabletree.operations.merge.statetransitions import (
    RequestTransition,
    ToStateTransition,
)


@dataclass
class MergeRequestObserver[U, E](RxSingleObserver[None, Never]):
    shared: MergeSharedMemory[U, E]

    @override
    def notify(
        self,
        notification: Notification[U, E],
    ) -> ContCert:
        match notification:
            case OnSuccess():
                transition = RequestTransition(
                    child=None,  # pyright: ignore[reportArgumentType]
                )

                with self.shared.transition_lock:
                    transition.child = self.shared.transition
                    n_state = transition.get_state()
                    self.shared.transition = ToStateTransition(state=n_state)

                match n_state:
                    case ActiveState(
                        received_ids=received_ids,
                    ):
                        if not received_ids:
                            match self.shared.items.pop():
                                case OnNextMergeItem():
                                    _, observer = self.shared.upstream.pop()
                                    cert = observer.notify(
                                        rxsingle.init_on_success(
                                            item=None,
                                            trampoline=notification.p_trampoline,
                                            item_scheduler=notification.p_item_scheduler,
                                        )
                                    )
                                    return cert

                                case _:
                                    with self.shared.certs_lock:
                                        cert = self.shared.certs.pop()
                                    return cert

                        else:
                            match self.shared.items.pop():
                                case OnNextMergeItem(item=next_item):
                                    _, observer = self.shared.upstream.pop()
                                    cert = observer.notify(
                                        rxsingle.init_on_success(
                                            item=None,
                                            trampoline=notification.p_trampoline,
                                            item_scheduler=notification.p_item_scheduler,
                                        )
                                    )
                                    with self.shared.certs_lock:
                                        self.shared.certs.append(cert)

                                    # print(f"request: next{item}")
                                    cert = self.shared.downstream.notify(
                                        init_on_next(
                                            item=next_item,
                                            trampoline=notification.p_trampoline,
                                            item_scheduler=notification.p_item_scheduler,
                                            observer=MergeRequestObserver(
                                                shared=self.shared,
                                            ),
                                        )
                                    )
                                    return cert

                                case OnNextAndCompleteMergeItem(item=next_item):
                                    cert = self.shared.downstream.notify(
                                        init_on_next_and_complete(
                                            item=next_item,
                                            trampoline=notification.p_trampoline,
                                            item_scheduler=notification.p_item_scheduler,
                                        )
                                    )
                                    return cert

                                case OnCompletedMergeItem():
                                    cert = self.shared.downstream.notify(
                                        init_on_completed(
                                            trampoline=notification.p_trampoline,
                                            item_scheduler=notification.p_item_scheduler,
                                        )
                                    )
                                    return cert

                                case _:
                                    raise Exception()

                    case StopContinuationState(index=index):
                        return self.shared.certs[index]

                    case ErrorState(msg=msg):
                        raise Exception(msg)

            case OnError():
                pass
