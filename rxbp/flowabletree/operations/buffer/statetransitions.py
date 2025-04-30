from abc import ABC, abstractmethod

from dataclassabc import dataclassabc

from continuationmonad.typing import ContinuationCertificate

from rxbp.flowabletree.operations.buffer.states import (
    CancelLoopState,
    LoopActive,
    BufferState,
    CancelledState,
    CompleteState,
    ErrorState,
    LoopActivePopBuffer,
    LoopInactive,
    SendErrorState,
    SendItemAndStartLoop,
    SendItemAndComplete,
    StopLoop,
)


class ShareStateTransition(ABC):
    @abstractmethod
    def get_state(self) -> BufferState: ...


@dataclassabc(frozen=True)
class ToStateTransition(ShareStateTransition):
    """Transitions to predefined state"""

    state: BufferState

    def get_state(self):
        return self.state


@dataclassabc(frozen=False)
class OnNextTransition(ShareStateTransition):
    """Item received"""

    child: ShareStateTransition
    certificate: ContinuationCertificate
    buffer_size: int | None

    def get_state(self):
        match child_state := self.child.get_state():
            case LoopActive(
                num_items=num_items,
                is_completed=is_completed,
                certificate=certificate,
            ):
                if self.buffer_size is not None and num_items == self.buffer_size:
                    return LoopActivePopBuffer(
                        num_items=num_items,
                        is_completed=is_completed,
                        certificate=certificate,
                    )

                else:
                    return LoopActive(
                        num_items=num_items + 1,
                        is_completed=is_completed,
                        certificate=certificate,
                    )

            case LoopInactive():
                return SendItemAndStartLoop(
                    num_items=1,
                    is_completed=False,
                    certificate=self.certificate,
                )

            case _:
                raise Exception(
                    f"Unexpected state {child_state} returned by action {self.child}."
                )


@dataclassabc(frozen=False)
class OnNextAndCompleteTransition(ShareStateTransition):
    """Item received, stream is complete"""

    child: ShareStateTransition
    buffer_size: int | None

    def get_state(self):
        match child_state := self.child.get_state():
            case LoopActive(
                num_items=num_items,
                certificate=certificate,
            ):
                if self.buffer_size is not None and num_items == self.buffer_size:
                    return LoopActivePopBuffer(
                        num_items=num_items,
                        is_completed=True,
                        certificate=certificate,
                    )

                else:
                    return LoopActive(
                        num_items=num_items + 1,
                        is_completed=True,
                        certificate=certificate,
                    )

            case LoopInactive():
                return SendItemAndComplete()

            case _:
                raise Exception(f"Unexpected state {child_state}.")


@dataclassabc
class RequestTransition(ShareStateTransition):
    """Downstream request received"""

    child: ShareStateTransition

    def get_state(self):
        match child_state := self.child.get_state():
            case LoopActive(
                num_items=num_items,
                is_completed=is_completed,
                certificate=certificate,
            ):
                if num_items == 0 and is_completed:
                    return CompleteState()

                if num_items == 0:
                    return StopLoop(certificate=certificate)

                if num_items == 1 and is_completed:
                    return SendItemAndComplete()

                else:
                    return LoopActive(
                        num_items=num_items - 1,
                        is_completed=is_completed,
                        certificate=certificate,
                    )

            case ErrorState(exception=exception):
                return SendErrorState(
                    exception=exception,
                )

            case _:
                raise Exception(f"Unexpected state {child_state}.")


@dataclassabc(frozen=False)
class OnCompletedTransition(ShareStateTransition):
    """Stream is complete"""

    child: ShareStateTransition

    def get_state(self):
        match child_state := self.child.get_state():
            case LoopActive(
                num_items=num_items,
                certificate=certificate,
            ):
                return LoopActive(
                    num_items=num_items,
                    is_completed=True,
                    certificate=certificate,
                )

            case LoopInactive():
                return CompleteState()

            case _:
                raise Exception(f"Unexpected state {child_state}.")


@dataclassabc(frozen=False)
class OnErrorTransition(ShareStateTransition):
    child: ShareStateTransition
    exception: Exception

    def get_state(self):
        match child_state := self.child.get_state():
            case LoopActive(certificate=certificate):
                return ErrorState(
                    exception=self.exception,
                    certificate=certificate,
                )

            case LoopInactive():
                return SendErrorState(
                    exception=self.exception,
                )

            case _:
                raise Exception(f"Unexpected state {child_state}.")


@dataclassabc(frozen=False)
class CancelTransition(ShareStateTransition):
    child: ShareStateTransition
    id: int

    def get_state(self):
        match child_state := self.child.get_state():
            case LoopActive(certificate=certificate):
                return CancelLoopState(certificate=certificate)

            case LoopInactive():
                return CancelledState()

            case _:
                raise Exception(f"Unexpected state {child_state}.")
