from abc import ABC
from typing import Optional, Any, Iterator

from rxbp.ack.acksubject import AckSubject
from rxbp.ack.mixins.ackmixin import AckMixin
from rxbp.selectors.selectionmsg import SelectionMsg


class ControlledZipStates:
    class ZipState(ABC):
        pass

    class WaitOnLeft(ZipState):
        """ Zip observable actor has or will back-pressure the left source, but no element
        has yet been received.

        In this state, the left buffer is empty.
        """

        def __init__(
                self,
                right_val: Any,
                right_ack: AckSubject,
                right_iter: Iterator,
                # right_sel: Optional[SelectionMsg],
                # left_sel: Optional[SelectionMsg],
                # right_sel_ack: Optional[AckMixin],
        ):
            self.right_val = right_val
            self.right_ack = right_ack
            self.right_iter = right_iter
            # self.right_sel = right_sel
            # self.left_sel = left_sel
            # self.right_sel_ack = right_sel_ack

    class WaitOnRight(ZipState):
        """ Equivalent of WaitOnLeft """

        def __init__(
                self,
                left_val: Any,
                left_ack: AckSubject,
                left_iter: Iterator,
                # right_sel: Optional[SelectionMsg],
                # left_sel: Optional[SelectionMsg],
                # left_sel_ack: Optional[AckMixin],
        ):
            self.left_val = left_val
            self.left_iter = left_iter
            self.left_ack = left_ack
            # self.right_sel = right_sel
            # self.left_sel = left_sel
            # self.left_sel_ack = left_sel_ack

    class WaitOnLeftRight(ZipState):
        """ Zip observable actor has or will back-pressure the left and right source, but
        no element has yet been received.

        In this state, the left and right buffer are empty.
        """

        # def __init__(
        #         self,
        #         right_sel: Optional[SelectionMsg],
        #         left_sel: Optional[SelectionMsg],
        # ):
        #     self.right_sel = right_sel
        #     self.left_sel = left_sel

    class ZipElements(ZipState):
        """ Zip observable actor is zipping the values just received by a source and
         from the buffer.

        In this state the actual termination state is ignored in the `get_measured_state`
        method.
        """

        def __init__(
                self,
                val: Any,
                is_left: bool,
                ack: AckSubject,
                iter: Iterator,
        ):
            self.val = val
            self.is_left = is_left
            self.ack = ack
            self.iter = iter

    class Stopped(ZipState):
        pass