from abc import ABC, abstractmethod
from dataclasses import replace

from dataclassabc import dataclassabc

from continuationmonad.typing import ContinuationCertificate
from rxbp.flowabletree.operations.share.states import (
    AckUpstream,
    ActiveState,
    AwaitOnNext,
    AwaitOnNextBase,
    CancelledState,
    CompleteState,
    ErrorState,
    SendItemFromBuffer,
    ShareState,
    SendItem,
    TerminatedBaseState,
    TerminatedState,
)


class ShareAction(ABC):
    @abstractmethod
    def get_state(self) -> ShareState: ...


@dataclassabc(frozen=True)
class FromStateAction(ShareAction):
    state: ShareState

    def get_state(self):
        return self.state



@dataclassabc(frozen=False)
class OnNextAndCompleteAction(ShareAction):
    child: ShareAction

    def get_state(self):
        match child_state := self.child.get_state():
            case AwaitOnNextBase(
                buffer_map=buffer_map,
                first_buffer_index=first_buffer_index,
                last_buffer_index=last_buffer_index,
                is_ack=is_ack,
                acc_certificate=acc_certificate,
            ):
                n_last_buffer_index = last_buffer_index + 1

                send_ids = tuple(id for id, index in buffer_map.items() if index == n_last_buffer_index)

                # buffer_item = first_buffer_index != last_buffer_index      # all subscribers request new item

                return SendItem(
                    buffer_map=buffer_map,
                    first_buffer_index=first_buffer_index,
                    last_buffer_index=n_last_buffer_index,
                    is_ack=is_ack,
                    acc_certificate=acc_certificate,
                    send_ids=send_ids,
                    buffer_item=False,
                    is_completed=True,
                )

            case CancelledState():
                return child_state

            case _:
                raise Exception(f"Unexpected state {child_state}.")

@dataclassabc(frozen=False)
class OnNextAction(ShareAction):
    child: ShareAction
    # upstream_ack_observer: DeferredObserver

    def get_state(self):
        match child_state := self.child.get_state():
            case AwaitOnNextBase(
                buffer_map=buffer_map,
                first_buffer_index=first_buffer_index,
                last_buffer_index=last_buffer_index,
                acc_certificate=acc_certificate,
            ):
                n_last_buffer_index = last_buffer_index + 1

                # print(buffer_map)
                send_ids = tuple(id for id, index in buffer_map.items() if index == n_last_buffer_index)

                buffer_item = first_buffer_index != n_last_buffer_index      # all subscribers request new item

                return SendItem(
                    buffer_map=buffer_map,
                    first_buffer_index=first_buffer_index,
                    last_buffer_index=n_last_buffer_index,
                    is_ack=False,
                    acc_certificate=acc_certificate,
                    send_ids=send_ids,
                    buffer_item=buffer_item,
                    is_completed=False,
                )

            case CancelledState():
                return child_state

            case _:
                raise Exception(f"Unexpected state {child_state} returned by action {self.child}.")


@dataclassabc
class AcknowledgeAction(ShareAction):
    child: ShareAction

    # downstream id
    id: int

    # downstream weight
    weight: int
    
    # subscription: DeferredSubscription

    def get_state(self):
        match child_state := self.child.get_state():
            case ActiveState(
                buffer_map=buffer_map,
                first_buffer_index=first_buffer_index,
                last_buffer_index=last_buffer_index,
                is_ack=is_ack,
                acc_certificate=acc_certificate,
                is_completed=is_completed,
            ):
                # update buffer_map
                n_buffer_map = {}
                for id, index in buffer_map.items():
                    if id == self.id:
                        sel_index = index
                        n_buffer_map[id] = index + 1

                    else:
                        n_buffer_map[id] = index

                if last_buffer_index == sel_index:
                    """ there is no item in the buffer """

                    # acc_certificate, certificate = acc_certificate.take(0)

                    if not is_ack:
                        return AckUpstream(
                            buffer_map=n_buffer_map,
                            first_buffer_index=first_buffer_index,
                            last_buffer_index=last_buffer_index,
                            is_ack=True,
                            acc_certificate=acc_certificate,
                            certificate=None,
                            is_completed=is_completed,
                        )
                
                    else:
                        certificate, acc_certificate = acc_certificate.take(self.weight)

                        return AwaitOnNext(
                            buffer_map=n_buffer_map,
                            first_buffer_index=first_buffer_index,
                            last_buffer_index=last_buffer_index,
                            is_ack=is_ack,
                            acc_certificate=acc_certificate,
                            certificate=certificate,
                            is_completed=is_completed,
                        )

                else:
                    """ there is an item in the buffer """
                
                    if sel_index == first_buffer_index:
                        # update last index
                        n_observers = sum(index for index in n_buffer_map.values() if index == first_buffer_index)
                        pop_item = n_observers == 0

                    else:
                        pop_item = False

                    return SendItemFromBuffer(
                        buffer_map=n_buffer_map,
                        first_buffer_index=first_buffer_index,
                        last_buffer_index=last_buffer_index,
                        is_ack=is_ack,
                        acc_certificate=acc_certificate,
                        is_completed=is_completed,
                        index=sel_index,
                        pop_item=pop_item,
                    )

            case TerminatedBaseState(exception=exception):
                return TerminatedState(exception=exception)

            case _:
                raise Exception(f"Unexpected state {child_state}.")


@dataclassabc(frozen=False)
class OnCompletedAction(ShareAction):
    child: ShareAction

    def get_state(self):
        match child_state := self.child.get_state():
            case ActiveState(
                buffer_map=buffer_map,
                last_buffer_index=last_buffer_index,
                acc_certificate=acc_certificate,
            ):
                send_ids = tuple(id for id, index in buffer_map.items() if index == last_buffer_index)

                return CompleteState(
                    send_ids=send_ids,
                    acc_certificate=acc_certificate,
                )

            case _:
                raise Exception(f"Unexpected state {child_state}.")


@dataclassabc(frozen=False)
class OnErrorAction(ShareAction):
    child: ShareAction
    exception: Exception

    def get_state(self):
        match child_state := self.child.get_state():
            case ActiveState(
                buffer_map=buffer_map,
                last_buffer_index=last_buffer_index,
                acc_certificate=acc_certificate,
            ):
                send_ids = tuple(id for id, index in buffer_map.items() if index == last_buffer_index)

                return ErrorState(
                    exception=self.exception,
                    send_ids=send_ids,
                    acc_certificate=acc_certificate,
                )

            case _:
                raise Exception(f"Unexpected state {child_state}.")


@dataclassabc(frozen=False)
class CancelAction(ShareAction):
    child: ShareAction
    id: int
    certificate: ContinuationCertificate

    def get_state(self):
        match child_state := self.child.get_state():
            case ActiveState(
                buffer_map=buffer_map, 
                first_buffer_index=first_buffer_index,
                last_buffer_index=last_buffer_index,
                is_ack=is_ack,
                acc_certificate=acc_certificate,
            ):

                # filter out downstream observer
                n_buffer_map = {id: index for id, index in buffer_map.items() if id != self.id}
                n_acc_certificate = self.certificate.merge((acc_certificate,))


                if len(n_buffer_map) == 0:
                    return CancelledState(
                        certificate=n_acc_certificate,
                    )
                
                else:
                    # return ActiveState(
                    #     buffer_map=n_buffer_map,
                    #     first_buffer_index=first_buffer_index,
                    #     last_buffer_index=last_buffer_index,
                    #     is_ack=is_ack,
                    #     acc_certificate=n_acc_certificate,
                    # )
                    replace(
                        child_state,
                        acc_certificate=n_acc_certificate,
                    )

            case _:
                raise Exception(f"Unexpected state {child_state}.")
