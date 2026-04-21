
from __future__ import annotations

from dataclasses import dataclass
from typing import override

import contcert
from contcert.typing import ContCert, Trampoline
from rxsingle.typing import Observer as RxSingleObserver

from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.share.states import Active, Error
from rxbp.flowabletree.operations.share.statetransitions import (
    RequestTransition,
    ToStateTransition,
)
from rxbp.flowabletree.operations.share.sharedmemory import ShareSharedMemory


@dataclass
class ShareRequestObserver[U](RxSingleObserver[None]):
    id: int
    downstream: Observer[U]
    shared: ShareSharedMemory[U]

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
                                    virtual_count=self.shared.virtual_count,
                                    context=self.shared.context,
                                    cancellation=None,
                                ),
                                stacks=self.shared.stacks,
                            )

                            with self.shared.cert_lock:
                                self.shared.cert = cert

        match n_state:
            case Active(
                buffer_map=buffer_map,
            ):
                # print(f'{self.shared.cert=}')

                with self.shared.cert_lock:
                    cert, self.shared.cert = contcert.take(
                        cert=self.shared.cert,
                        virtual_count=self.shared.virtual_count_partition[self.id],
                        stacks=self.shared.stacks,
                        lambda_trace_logger=self.shared.lambda_trace_logger,
                    )

                # print(f'{cert=}')

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
