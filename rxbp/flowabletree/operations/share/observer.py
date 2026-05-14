from __future__ import annotations

from dataclasses import dataclass
from typing import override

import contcert
from contcert.typing import ContCert

from rxbp.cancellable import CancellationState
from rxbp.flowabletree.observer import Observer
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
from rxbp.flowabletree.operations.share.states import ActiveState, ErrorState
from rxbp.flowabletree.operations.share.statetransitions import (
    OnNextTransition,
    OnNextAndCompleteTransition,
    OnCompletedTransition,
)
from rxbp.flowabletree.operations.share.sharedmemory import ShareSharedMemory
from rxbp.flowabletree.operations.share.requestobserver import ShareRequestObserver


@dataclass
class ShareObserver[U, E](Observer[U, E]):
    shared: ShareSharedMemory[U, E]
    request_observers: dict[int, ShareRequestObserver[U, E]]
    cancellations: dict[int, CancellationState]

    @override
    def notify(
        self,
        notification: Notification[U, E],
    ) -> ContCert:
        match notification:
            case OnNext():
                # print(f"on_next({notification.p_item})")

                self.shared.buffer.append(notification.p_item)
                self.shared.upstream = notification.p_observer

                transition = OnNextTransition(
                    child=None,  # pyright: ignore[reportArgumentType]
                )

                with self.shared.transition_lock:
                    transition.child = self.shared.transition
                    self.shared.transition = transition

                match state := transition.get_state():
                    case ActiveState(
                        buffer_map=buffer_map,
                    ):
                        last_index = buffer_map.next_index - 1

                        certs: list[ContCert] = [self.shared.cert] + list(state.cancelled_certs)
                        # print(state)

                        # send item ready downstream
                        for id, idx in buffer_map.mapping.items():
                            if idx == last_index:
                                downstream = self.request_observers[id]
                                cert = downstream.downstream.notify(
                                    init_on_next(
                                        item=notification.p_item,
                                        trampoline=notification.p_trampoline,
                                        item_scheduler=notification.p_item_scheduler,
                                        observer=downstream,
                                    )
                                )
                                certs.append(cert)

                        return contcert.merge(
                            certs,
                            stacks=self.shared.stacks,
                            lambda_trace_logger=self.shared.lambda_trace_logger,
                        )

                    case ErrorState(msg=msg):
                        raise Exception(msg)

            case OnNextAndComplete():
                # print(f"on_next_and_complete({notification.p_item})")

                transition = OnNextAndCompleteTransition(
                    child=None,  # pyright: ignore[reportArgumentType]
                )

                with self.shared.transition_lock:
                    transition.child = self.shared.transition
                    self.shared.transition = transition

                match state := transition.get_state():
                    case ActiveState(
                        buffer_map=buffer_map,
                    ):
                        last_index = buffer_map.next_index

                        certs: list[ContCert] = [self.shared.cert] + list(state.cancelled_certs)

                        # send
                        for id, idx in buffer_map.mapping.items():
                            if idx == last_index:
                                downstream = self.request_observers[id]
                                cert = downstream.downstream.notify(
                                    init_on_next_and_complete(
                                        item=notification.p_item,
                                        trampoline=notification.p_trampoline,
                                        item_scheduler=notification.p_item_scheduler,
                                    )
                                )
                                certs.append(cert)

                        return contcert.merge(
                            certs,
                            stacks=self.shared.stacks,
                            lambda_trace_logger=self.shared.lambda_trace_logger,
                        )

                    case ErrorState(msg=msg):
                        raise Exception(msg)

            case OnCompleted():
                transition = OnCompletedTransition(
                    child=None,  # type: ignore
                )

                with self.shared.transition_lock:
                    transition.child = self.shared.transition
                    self.shared.transition = transition

                match state := transition.get_state():
                    case ActiveState(
                        buffer_map=buffer_map,
                    ):
                        last_index = buffer_map.next_index

                        certs: list[ContCert] = [self.shared.cert] + list(state.cancelled_certs)

                        # send
                        for id, idx in buffer_map.mapping.items():
                            if idx == last_index:
                                downstream = self.request_observers[id]
                                cert = downstream.downstream.notify(
                                    init_on_completed(
                                        trampoline=notification.p_trampoline,
                                        item_scheduler=notification.p_item_scheduler,
                                    )
                                )
                                certs.append(cert)

                        return contcert.merge(
                            certs,
                            stacks=self.shared.stacks,
                            lambda_trace_logger=self.shared.lambda_trace_logger,
                        )

                    case ErrorState(msg=msg):
                        raise Exception(msg)

            case OnError():
                transition = OnCompletedTransition(
                    child=None,  # pyright: ignore[reportArgumentType]
                )

                with self.shared.transition_lock:
                    transition.child = self.shared.transition
                    self.shared.transition = transition

                match state := transition.get_state():
                    case ActiveState(
                        buffer_map=buffer_map,
                    ):
                        last_index = buffer_map.next_index - 1

                        certs: list[ContCert] = [self.shared.cert] + list(state.cancelled_certs)

                        # send
                        for id, idx in buffer_map.mapping.items():
                            if idx == last_index:
                                downstream = self.request_observers[id]
                                cert = downstream.downstream.notify(
                                    init_on_error(
                                        exception=notification.p_exception,
                                        trampoline=notification.p_trampoline,
                                        item_scheduler=notification.p_item_scheduler,
                                    )
                                )
                                certs.append(cert)

                        return contcert.merge(
                            certs,
                            stacks=self.shared.stacks,
                            lambda_trace_logger=self.shared.lambda_trace_logger,
                        )

                    case ErrorState(msg=msg):
                        raise Exception(msg)
