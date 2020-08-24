from traceback import FrameSummary
from typing import Callable, List

from rx.internal import SequenceContainsNoElementsError

from rxbp.ack.continueack import continue_ack
from rxbp.ack.stopack import stop_ack
from rxbp.observer import Observer
from rxbp.typing import ElementType
from rxbp.utils.tooperatorexception import to_operator_exception


class LastObserver(Observer):
    def __init__(
            self,
            observer: Observer,
            stack: List[FrameSummary],
    ):
        self.observer = observer
        self.stack = stack

        self.has_value = False
        self.last_value = None

    def on_next(self, elem: ElementType):

        if isinstance(elem, list):
            elem_list = elem

        else:
            try:
                elem_list = list(elem)
            except Exception as exc:
                self.observer.on_error(exc)
                return stop_ack

        if 0 < len(elem_list):
            self.has_value = True
            self.last_value = elem_list[-1]

        return continue_ack

    def on_error(self, exc):
        return self.observer.on_error(exc)

    def on_completed(self):
        if self.has_value:
            self.observer.on_next([self.last_value])
            self.observer.on_completed()

        else:
            pass
            try:
                raise SequenceContainsNoElementsError(to_operator_exception(
                    message='',
                    stack=self.stack,
                ))

            except Exception as exc:
                self.observer.on_error(exc)
