from abc import ABC

from rxbp.ack.acksubject import AckSubject
from rxbp.typing import ElementType


class MergeStates:
    class MergeState(ABC):
        pass

    class NoneReceived(MergeState):
        pass

    class NoneReceivedWaitAck(MergeState):
        pass

    class SingleReceived(MergeState, ABC):
        def __init__(self, elem: ElementType, ack: AckSubject):
            self.elem = elem
            self.ack = ack

    class LeftReceived(SingleReceived):
        pass

    class RightReceived(SingleReceived):
        pass

    class BothReceived(MergeState, ABC):
        def __init__(
                self,
                left_elem: ElementType,
                right_elem: ElementType,
                left_ack: AckSubject,
                right_ack: AckSubject,
        ):
            self.left_elem = left_elem
            self.right_elem = right_elem
            self.left_ack = left_ack
            self.right_ack = right_ack

    class BothReceivedContinueLeft(BothReceived):
        pass

    class BothReceivedContinueRight(BothReceived):
        pass

    class Stopped(MergeState):
        pass
