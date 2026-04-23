from __future__ import annotations

from dataclasses import dataclass
from typing import override

import contcert
from contcert.typing import ContCert, Trampoline
from rxsingle.typing import Observer as RxSingleObserver

from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.asyncshare.states import Active, Error
from rxbp.flowabletree.operations.asyncshare.statetransitions import (
    RequestTransition,
    ToStateTransition,
)
from rxbp.flowabletree.operations.asyncshare.sharedmemory import AsyncShareSharedMemory


@dataclass
class AsyncShareRequestObserver[U](RxSingleObserver[None]):
    id: int
    downstream: Observer[U]
    shared: AsyncShareSharedMemory[U]
    virtual_count: int
    context: int

    @override
    def on_success(
        self,
        trampoline: Trampoline,
        item: None,
    ):
        transition = RequestTransition(
            child=None,  # pyright: ignore[reportArgumentType]
            id=self.id,
        )

        with self.shared.transition_lock:
            transition.child = self.shared.transition
            n_state = transition.get_state()
            self.shared.transition = ToStateTransition(state=n_state)

            match n_state:
                case Active(
                    buffer_map=buffer_map,
                ):
                    index = buffer_map.mapping[self.id]

                    # await next upstream item
                    if index == buffer_map.next_index :
                        n_observers = sum(
                            1
                            for index in buffer_map.mapping.values()
                            if index == (buffer_map.next_index)
                        )

                        # request next item from upstream
                        if n_observers == 1:

                            def request_next(self=self) -> ContCert:
                                cert = self.shared.upstream.on_success(trampoline, None)
                                return cert

                            cert = trampoline.schedule(
                                contcert.init_task(
                                    callable=request_next,
                                    # virtual_count=self.shared.virtual_count,
                                    virtual_count=1,
                                    context=self.shared.context,
                                    cancellation=None,
                                ),
                                stacks=self.shared.stacks,
                            )

                            self.shared.context_translation = contcert.init_context_translation(
                                cert=cert,
                                lambda_trace_logger=self.shared.lambda_trace_logger,
                            )

        match n_state:
            case Active(
                buffer_map=buffer_map,
            ):
                # print(f'{self.shared.cert=}')

                # print(self.shared.context_translation.transition.get_state())
                # print('increase')
                cert = self.shared.context_translation.increase(
                    virtual_count=self.virtual_count,
                    context=self.context,
                    stacks=tuple(),
                    lambda_trace_logger=self.shared.lambda_trace_logger,
                )

                # print(self.shared.context_translation.transition.get_state())
                return cert

            case Error(msg=msg):
                raise Exception(msg)

    @override
    def on_error(
        self,
        trampoline: Trampoline,
        exception: Exception,
    ):
        raise NotImplementedError
