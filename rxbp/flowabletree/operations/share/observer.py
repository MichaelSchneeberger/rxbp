from __future__ import annotations

from dataclasses import dataclass
from typing import override

import contcert
from contcert.typing import ContCert, Trampoline
from rxsingle.typing import Observer as RxSingleObserver

from rxbp.cancellable import CancellationState
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.share.states import Active, Error
from rxbp.flowabletree.operations.share.statetransitions import (
    OnNextTransition,
    OnNextAndCompleteTransition,
    OnCompletedTransition,
)
from rxbp.flowabletree.operations.share.sharedmemory import ShareSharedMemory
from rxbp.flowabletree.operations.share.requestobserver import ShareRequestObserver


@dataclass
class SharedObserver[U](Observer[U]):
    shared: ShareSharedMemory[U]
    request_observers: dict[int, ShareRequestObserver[U]]
    cancellations: dict[int, CancellationState]

    @override
    def on_next(
        self, trampoline: Trampoline, item: U, observer: RxSingleObserver[None]
    ) -> ContCert:
        print(f"on_next({item})")

        self.shared.buffer.append(item)
        self.shared.upstream = observer

        transition = OnNextTransition(
            child=None,  # pyright: ignore[reportArgumentType]
        )

        with self.shared.transition_lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case Active(
                buffer_map=buffer_map,
            ):
                # assert state.send_ids
                last_index = buffer_map.next_index

                # self.shared.cert is not modified after OnNext Transition
                # until the downstream.on_next is called
                certs: list[ContCert] = [self.shared.cert] + list(state.cancelled_certs)

                # send item ready downstream
                for id, idx in buffer_map.mapping.items():
                    if idx == last_index:
                        downstream = self.request_observers[id]
                        cert = downstream.downstream.on_next(trampoline, item, downstream)
                        certs.append(cert)

                return contcert.merge(
                    certs,
                    stacks=self.shared.stacks,
                    lambda_trace_logger=self.shared.lambda_trace_logger,
                )

            case Error(msg=msg):
                raise Exception(msg)

    @override
    def on_next_and_complete(self, trampoline: Trampoline, item: U) -> ContCert:
        print(f"on_next_and_complete({item})")

        transition = OnNextAndCompleteTransition(
            child=None,  # pyright: ignore[reportArgumentType]
        )

        with self.shared.transition_lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case Active(
                buffer_map=buffer_map,
            ):
                # assert state.send_ids
                last_index = buffer_map.next_index

                certs: list[ContCert] = [self.shared.cert] + list(state.cancelled_certs)

                # print(f'{buffer_map.mapping=}')
                # print(last_index)

                # send
                for id, idx in buffer_map.mapping.items():
                    if idx == last_index:
                        downstream = self.request_observers[id]
                        # print(f'{downstream=}')
                        cert = downstream.downstream.on_next_and_complete(
                            trampoline, item
                        )
                        certs.append(cert)

                # print(f'{certs=}')

                return contcert.merge(
                    certs,
                    stacks=self.shared.stacks,
                    lambda_trace_logger=self.shared.lambda_trace_logger,
                )

            case Error(msg=msg):
                raise Exception(msg)

    @override
    def on_completed(self) -> ContCert:
        transition = OnCompletedTransition(
            child=None,  # type: ignore
        )

        with self.shared.transition_lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case Active(
                buffer_map=buffer_map,
            ):
                # assert state.send_ids
                last_index = buffer_map.next_index

                certs: list[ContCert] = [self.shared.cert] + list(state.cancelled_certs)

                # send
                for id, idx in buffer_map.mapping.items():
                    if idx == last_index:
                        downstream = self.request_observers[id]
                        cert = downstream.downstream.on_completed()
                        certs.append(cert)

                return contcert.merge(
                    certs,
                    stacks=self.shared.stacks,
                    lambda_trace_logger=self.shared.lambda_trace_logger,
                )

            case Error(msg=msg):
                raise Exception(msg)

    @override
    def on_error(self, exception: Exception) -> ContCert:
        transition = OnCompletedTransition(
            child=None,  # pyright: ignore[reportArgumentType]
        )

        with self.shared.transition_lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case Active(
                buffer_map=buffer_map,
            ):
                # assert state.send_ids
                last_index = buffer_map.next_index - 1

                certs: list[ContCert] = [self.shared.cert] + list(state.cancelled_certs)

                # send
                for id, idx in buffer_map.mapping.items():
                    if idx == last_index:
                        downstream = self.request_observers[id]
                        cert = downstream.downstream.on_error(exception)
                        certs.append(cert)

                return contcert.merge(
                    certs,
                    stacks=self.shared.stacks,
                    lambda_trace_logger=self.shared.lambda_trace_logger,
                )

            case Error(msg=msg):
                raise Exception(msg)
