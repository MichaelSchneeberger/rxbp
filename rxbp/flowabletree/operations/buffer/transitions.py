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
    LoopInactive,
    SendErrorState,
    SendItemAndStartLoop,
    SendItemAndComplete,
    StopLoop,
)


class ShareTransition(ABC):
    @abstractmethod
    def get_state(self) -> BufferState: ...


@dataclassabc(frozen=True)
class ToStateTransition(ShareTransition):
    """Transitions to predefined state"""

    state: BufferState

    def get_state(self):
        return self.state


@dataclassabc(frozen=False)
class OnNextTransition(ShareTransition):
    """Item received"""

    child: ShareTransition
    certificate: ContinuationCertificate

    def get_state(self):
        match child_state := self.child.get_state():

            case LoopActive(
                num_items=num_items,
                is_completed=is_completed,
                certificate=certificate,
            ):
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
class OnNextAndCompleteTransition(ShareTransition):
    """Item received, stream is complete"""

    child: ShareTransition

    def get_state(self):
        match child_state := self.child.get_state():

            case LoopActive(
                num_items=num_items,
                certificate=certificate,
            ):
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
class RequestTransition(ShareTransition):
    """Downstream request received"""

    child: ShareTransition

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
class OnCompletedTransition(ShareTransition):
    """Stream is complete"""

    child: ShareTransition

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
class OnErrorTransition(ShareTransition):
    child: ShareTransition
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
class CancelTransition(ShareTransition):
    child: ShareTransition
    id: int
    # certificate: ContinuationCertificate

    def get_state(self):
        match child_state := self.child.get_state():
            case LoopActive(certificate=certificate):
                return CancelLoopState(certificate=certificate)
            
            case LoopInactive():
                return CancelledState()

            case _:
                raise Exception(f"Unexpected state {child_state}.")
