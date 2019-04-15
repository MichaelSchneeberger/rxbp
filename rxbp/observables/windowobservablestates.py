from typing import Any, Iterator, Optional

from rxbp.ack import Ack
from rxbp.subjects.publishsubject import PublishSubject


class WindowObservableStates:
    class State:
        pass


    class InitialState(State):
        pass


    class WaitOnLeft(State):
        def __init__(self,
                     right_val: Any,
                     right_iter: Iterator[Any],
                     right_in_ack: Ack,
                     ):
            self.right_val = right_val
            self.right_iter = right_iter
            self.right_in_ack = right_in_ack


    class WaitOnRight(State):
        def __init__(self,
                     val_subject_iter: Iterator,
                     left_val: Any,
                     subject: PublishSubject,
                     left_in_ack: Ack,
                     last_left_out_ack: Optional[Ack]):
            self.left_val = left_val
            self.subject = subject
            self.val_subject_iter = val_subject_iter
            self.left_in_ack = left_in_ack
            self.last_left_out_ack = last_left_out_ack


    class Transition(State):
        pass


    class Completed(State):
        pass


    class ConcurrentType:
        pass


    class SynchronousLeft(ConcurrentType):
        """ Object indicating if function is called synchronously
        """

        def __init__(self, right_in_ack: Ack):
            self.right_in_ack = right_in_ack


    class SynchronousRight(ConcurrentType):
        """ Object indicating if function is called synchronously
        """

        def __init__(self, left_in_ack: Ack, left_out_ack: Ack):
            self.left_in_ack = left_in_ack
            self.left_out_ack = left_out_ack


    class Asynchronous(ConcurrentType):
        """ Object indicating if function is called asynchronously
        """

        def __init__(self, left_in_ack: Ack, last_left_out_ack: Optional[Ack],
                     right_in_ack: Ack):
            self.left_in_ack = left_in_ack
            self.last_left_out_ack = last_left_out_ack
            self.right_in_ack = right_in_ack


    class OnLeftLowerState:
        pass


    class SyncMoreLeftOLL(OnLeftLowerState):
        """ on_left_lower is called synchronously from iterate_over_right
        At least one left value is lower than current right value,
        there are possible more left values in iterable.
        """

        def __init__(self, left_val, last_left_out_ack, subject):
            self.left_val = left_val
            self.last_left_out_ack = last_left_out_ack
            self.subject = subject


    class SyncNoMoreLeftOLL(OnLeftLowerState):
        """ on_left_lower is called synchronously from iterate_over_right
        No left value is lower than current right value, left iterable is empty
        """

        def __init__(self, left_in_ack, right_in_ack):
            self.left_in_ack = left_in_ack
            self.right_in_ack = right_in_ack


    class AsyncOLL(OnLeftLowerState):
        """ Crossing asynchronous border when sending left values
        """

        def __init__(self, left_in_ack, right_in_ack):
            self.left_in_ack = left_in_ack
            self.right_in_ack = right_in_ack
