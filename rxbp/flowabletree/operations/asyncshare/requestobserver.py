from __future__ import annotations

from dataclasses import dataclass
from typing import Never, override

import contcert
from contcert.typing import ContCert
from rxbp.flowabletree.notifications import init_on_next
from rxsingle.typing import (
    Notification as RxSingleNotification,
    Observer as RxSingleObserver,
    OnSuccess as RxSingleOnSuccess,
    OnError as RxSingleOnError,
)
import rxsingle

from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.asyncshare.states import ActiveState, ErrorState
from rxbp.flowabletree.operations.asyncshare.statetransitions import (
    RequestTransition,
    ToStateTransition,
)
from rxbp.flowabletree.operations.asyncshare.sharedmemory import AsyncShareSharedMemory


@dataclass
class AsyncShareRequestObserver[U, E](RxSingleObserver[None, Never]):
    id: int
    downstream: Observer[U, E]
    shared: AsyncShareSharedMemory[U, E]

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
                    self.shared.transition = ToStateTransition(state=n_state)

                    match n_state:
                        case ActiveState(
                            buffer_map=buffer_map,
                        ):
                            index = buffer_map.mapping[self.id]

                            # await next upstream item
                            if index == buffer_map.next_index:
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
                                            virtual_count=2,
                                            async_index=self.shared.async_index,
                                            cancellation=None,
                                        ),
                                        stacks=self.shared.stacks,
                                    )

                                    cert1, cert2 = contcert.take(
                                        cert,
                                        1,
                                        stacks=tuple(),
                                        lambda_trace_logger=self.shared.lambda_trace_logger,
                                    )

                                    self.shared.async_translation = contcert.init_async_index_translation(
                                        cert=cert1,
                                        lambda_trace_logger=self.shared.lambda_trace_logger,
                                    )
                                    self.shared.cert = cert2

                match n_state:
                    case ActiveState(
                        buffer_map=buffer_map,
                        is_completed=is_completed,
                    ):
                        index = buffer_map.mapping[self.id]

                        # await next upstream item
                        if index == buffer_map.next_index:
                            if self.id == 0:
                                cert = self.shared.cert
                                self.shared.cert = contcert.empty_cert(
                                    async_index=self.shared.async_index,
                                    stacks=tuple(),
                                )
                                return cert
                            else:
                                cert = self.shared.async_translation.increase(
                                    virtual_count=self.shared.virtual_count_partition[
                                        self.id
                                    ],
                                    async_index=self.shared.async_index_partition[self.id],
                                    stacks=self.shared.stacks,
                                    lambda_trace_logger=self.shared.lambda_trace_logger,
                                )

                                return cert
                        else:
                            with self.shared.buffer_lock:
                                item = self.shared.buffer[self.shared.first_index + index - 1]

                            n_observers = sum(
                                1
                                for idx in buffer_map.mapping.values()
                                if idx == (buffer_map.first_index)
                            )

                            if n_observers == 0:
                                with self.shared.buffer_lock:
                                    self.shared.buffer.pop()
                                    self.shared.first_index = self.shared.first_index + 1

                            if index == buffer_map.next_index - 1 and is_completed:
                                pass

                            else:
                                return self.downstream.notify(
                                    init_on_next(
                                        item=item,
                                        trampoline=notification.p_trampoline,
                                        item_scheduler=notification.p_item_scheduler,
                                        observer=self,
                                    )
                                )

                    case ErrorState(msg=msg):
                        raise Exception(msg)

            case RxSingleOnError():
                # todo: handle error
                pass
