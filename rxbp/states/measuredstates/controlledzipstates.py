from abc import ABC
from dataclasses import dataclass
from typing import Any, Iterator

from rxbp.ack.acksubject import AckSubject


class ControlledZipStates:
    class ZipState(ABC):
        pass

    @dataclass
    class WaitOnLeft(ZipState):
        """ Zip observable actor has or will back-pressure the left source, but no element
        has yet been received.

        In this state, the left buffer is empty.
        """

        right_val: Any
        right_ack: AckSubject
        right_iter: Iterator

    @dataclass
    class WaitOnRight(ZipState):
        """ Equivalent to WaitOnLeft """

        left_val: Any
        left_ack: AckSubject
        left_iter: Iterator

    class WaitOnLeftRight(ZipState):
        """ Zip observable actor has or will back-pressure the left and right source, but
        no element has yet been received.

        In this state, the left and right buffer are empty.
        """

        pass

    @dataclass
    class ZipElements(ZipState):
        """ Zip observable actor is zipping the values just received by a source and
         from the buffer.

        In this state the actual termination state is ignored in the `get_measured_state`
        method.
        """

        val: Any
        is_left: bool
        ack: AckSubject
        iter: Iterator

    class Stopped(ZipState):
        pass
