from __future__ import annotations

from dataclasses import dataclass
from threading import Lock

from donotation import do

import continuationmonad
from continuationmonad.typing import (
    DeferredHandler,
    ContinuationCertificate,
)

from rxbp.cancellable import Cancellable, CancellationState
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.buffer.states import (
    CancelLoopState,
    CancelledState,
    CompleteState,
    ErrorState,
    LoopActive,
    LoopActivePopBuffer,
    SendErrorState,
    SendItemAndComplete,
    SendItemAndStartLoop,
    StopLoop,
)
from rxbp.flowabletree.operations.buffer.statetransitions import (
    CancelTransition,
    OnErrorTransition,
    OnNextTransition,
    OnNextAndCompleteTransition,
    OnCompletedTransition,
    RequestTransition,
    ShareStateTransition,
)


@dataclass
class BufferObserver[V](Cancellable, Observer[V]):
    observer: Observer
    upstream_cancellable: Cancellable
    loop_cancellation: CancellationState
    weight: int
    buffer_size: int | None

    transition: ShareStateTransition
    lock: Lock

    buffer: list[V]

    @do()
    def run(self):
        transition = RequestTransition(child=None)

        with self.lock:
            transition.child = self.transition
            self.transition = transition

        state = transition.get_state()

        match state:
            case LoopActive():
                item = self.buffer.pop(0)
                _ = yield from self.observer.on_next(item)
                return self.run()

            case SendItemAndComplete():
                item = self.buffer.pop(0)
                return self.observer.on_next_and_complete(item)

            case StopLoop(certificate=certificate):
                return continuationmonad.from_(certificate)

            case CompleteState():
                return self.observer.on_completed()

            case SendErrorState(exception=exception):
                return self.observer.on_error(exception)

            case _:
                raise Exception(f"Unexpected state {state}")

    def on_next(self, value: V):
        # print(f"on_next({value})")

        self.buffer.append(value)

        @do()
        def on_ack_subscription(_, handler: DeferredHandler):
            # print(f"on_next_subscription({value})")

            certificate, *_ = yield from continuationmonad.from_(None).connect(
                (handler,)
            )

            transition = OnNextTransition(
                child=None,  # type: ignore
                certificate=certificate,
                buffer_size=self.buffer_size,
            )

            with self.lock:
                transition.child = self.transition
                self.transition = transition

            state = transition.get_state()

            match state:
                case SendItemAndStartLoop():
                    trampoline = yield from continuationmonad.get_trampoline()

                    r_certificate = continuationmonad.fork(
                        source=self.run(),
                        on_error=lambda e: self.observer.on_error(e),
                        scheduler=trampoline,
                        cancellation=self.loop_cancellation,
                        weight=self.weight,
                    )

                case LoopActivePopBuffer():
                    # pop item that has just been added
                    self.buffer.pop()
                    
                    r_certificate = certificate

                case LoopActive():
                    r_certificate = certificate

                case _:
                    raise Exception(f"Unexpected state {state}")

            return continuationmonad.from_(r_certificate)

        return continuationmonad.defer(on_ack_subscription)

    @do()
    def on_next_and_complete(self, value: V):
        # print(f"on_next_and_complete({value})")

        self.buffer.append(value)

        transition = OnNextAndCompleteTransition(
            child=None,  # type: ignore
            buffer_size=self.buffer_size,
        )

        with self.lock:
            transition.child = self.transition
            self.transition = transition

        match state := transition.get_state():
            case LoopActivePopBuffer(certificate=certificate):
                self.buffer.pop(0)
                return continuationmonad.from_(certificate)

            case LoopActive(certificate=certificate):
                # there are still items in the buffer
                return continuationmonad.from_(certificate)

            case SendItemAndComplete():
                return self.observer.on_next_and_complete(value)

            case _:
                raise Exception(f"Unexpected state {state}")

    def on_completed(self):
        transition = OnCompletedTransition(
            child=None,  # type: ignore
        )

        with self.lock:
            transition.child = self.transition
            self.transition = transition

        match state := transition.get_state():
            case CompleteState():
                return self.observer.on_completed()

            case LoopActive(certificate=certificate):
                return continuationmonad.from_(certificate)

            case _:
                raise Exception(f"Unexpected state {state}.")

    def on_error(self, exception: Exception):
        transition = OnErrorTransition(
            child=None,  # type: ignore
            exception=exception,
        )

        with self.lock:
            transition.child = self.transition
            self.transition = transition

        match state := transition.get_state():
            case SendErrorState(
                exception=exception,
            ):
                return self.observer.on_error(exception)

            case ErrorState(certificate=certificate):
                return continuationmonad.from_(certificate)

            case _:
                raise Exception(f"Unexpected state {state}.")

    def cancel(self, certificate: ContinuationCertificate):
        transition = CancelTransition(
            child=None,  # type: ignore
        )

        with self.lock:
            transition.child = self.transition
            self.transition = transition

        self.upstream_cancellable.cancel(certificate=certificate)

        match state := transition.get_state():
            case CancelLoopState(certificate=certificate):
                self.loop_cancellation.cancel(certificate=certificate)

            case CancelledState():
                pass

            case _:
                raise Exception(f"Unexpected state {state}.")
