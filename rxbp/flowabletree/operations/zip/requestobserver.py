
from __future__ import annotations

import rxsingle
from collections import deque
from dataclasses import dataclass
from typing import Never, override

from contcert.typing import ContCert
from rxsingle.typing import (
    Notification as RxSingleNotification,
    Observer as RxSingleObserver,
    OnSuccess as RxSingleOnSuccess,
    OnError as RxSingleOnError,
)

from rxbp.flowabletree.operations.zip.states import (
    ActiveState,
    CompletedState,
    ErrorState,
)
from rxbp.flowabletree.operations.zip.statetransitions import (
    RequestTransition,
    ToStateTransition,
)
from rxbp.flowabletree.operations.zip.sharedmemory import ZipSharedMemory


@dataclass
class ZipRequestObserver[U, A, E](RxSingleObserver[None, Never]):
    shared: ZipSharedMemory[U, A, E]
    requested_ids: set[int]

    @override
    def notify(
        self,
        notification: RxSingleNotification[None, Never],
    ) -> ContCert:
        match notification:
            case RxSingleOnSuccess():
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
                    case ActiveState():
                        pass

                    case CompletedState():
                        # todo: assure a single certificate
                        return self.shared.certs[-1]

                    case ErrorState(msg=msg):
                        raise Exception(msg)

                n_upstream: deque[tuple[int, RxSingleObserver[None, Never]]] = deque()
                certs: list[ContCert] = []
                for id, observer in self.shared.upstream:
                    if id in self.requested_ids:
                        cert = observer.notify(
                            rxsingle.init_on_success(
                                item=None,
                                trampoline=notification.p_trampoline,
                                item_scheduler=notification.p_item_scheduler,
                            )
                        )
                        certs.append(cert)

                    else:
                        n_upstream.append((id, observer))

                self.shared.items = deque()
                self.shared.upstream = n_upstream
                self.shared.certs = certs[1:]

                return certs[0]

            case RxSingleOnError():
                # todo: handle error
                pass
