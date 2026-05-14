from __future__ import annotations

from dataclasses import dataclass
from typing import Never, override

import contcert
from contcert.typing import ContCert
from rxsingle.typing import (
    Notification as RxSingleNotification,
    Observer as RxSingleObserver,
    OnSuccess as RxSingleOnSuccess,
    OnError as RxSingleOnError,
)
import rxsingle

from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.share.states import ActiveState, ErrorState
from rxbp.flowabletree.operations.share.statetransitions import (
    RequestTransition,
    ToStateTransition,
)
from rxbp.flowabletree.operations.share.sharedmemory import ShareSharedMemory


@dataclass
class ShareRequestObserver[U, E](RxSingleObserver[None, Never]):
    id: int
    downstream: Observer[U, E]
    shared: ShareSharedMemory[U, E]

    @override
    def notify(
        self,
        notification: RxSingleNotification[None, Never],
    ) -> ContCert:
        match notification:
            case RxSingleOnSuccess():
                transition = RequestTransition(
                    child=None,  # pyright: ignore[reportArgumentType]
                    id=self.id,
                )

                with self.shared.transition_lock:
                    transition.child = self.shared.transition
                    n_state = transition.get_state()
                    # print(n_state)
                    self.shared.transition = ToStateTransition(state=n_state)

                    match n_state:
                        case ActiveState(
                            buffer_map=buffer_map,
                        ):
                            index = buffer_map.mapping[self.id]

                            # await next upstream item
                            if index == buffer_map.next_index :
                                n_observers = sum(
                                    1
                                    for idx in buffer_map.mapping.values()
                                    if idx == (buffer_map.next_index)
                                )

                                # request next item from upstream
                                if n_observers == 1:

                                    def request_next(self=self) -> ContCert:
                                        cert = self.shared.upstream.notify(
                                            rxsingle.init_on_success(
                                                item=None,
                                                trampoline=notification.p_trampoline,
                                                item_scheduler=notification.p_item_scheduler,
                                            )
                                        )
                                        return cert

                                    cert = notification.p_trampoline.schedule(
                                        contcert.init_task(
                                            callable=request_next,
                                            virtual_count=self.shared.virtual_count,
                                            async_index=self.shared.context,
                                            cancellation=None,
                                        ),
                                        stacks=self.shared.stacks,
                                    )

                                    with self.shared.cert_lock:
                                        self.shared.cert = cert

                match n_state:
                    case ActiveState():
                        with self.shared.cert_lock:
                            cert, self.shared.cert = contcert.take(
                                cert=self.shared.cert,
                                virtual_count=self.shared.virtual_count_partition[self.id],
                                stacks=self.shared.stacks,
                                lambda_trace_logger=self.shared.lambda_trace_logger,
                            )

                        return cert

                    case ErrorState(msg=msg):
                        raise Exception(msg)

            case RxSingleOnError():
                # todo: handle error
                pass
