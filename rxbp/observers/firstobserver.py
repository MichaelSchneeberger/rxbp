from dataclasses import dataclass
from traceback import FrameSummary
from typing import List

from rx.internal import SequenceContainsNoElementsError

from rxbp.acknowledgement.continueack import continue_ack
from rxbp.acknowledgement.stopack import stop_ack
from rxbp.observer import Observer
from rxbp.typing import ElementType
from rxbp.utils.tooperatorexception import to_operator_exception


@dataclass
class FirstObserver(Observer):
    observer: Observer
    stack: List[FrameSummary]

    def __post_init__(self):
        self.is_first = True

    def on_next(self, elem: ElementType):
        try:
            first_elem = next(iter(elem))
        except StopIteration:
            return continue_ack

        self.is_first = False
        self.observer.on_next([first_elem])
        self.observer.on_completed()

        return stop_ack

    def on_error(self, exc):
        return self.observer.on_error(exc)

    def on_completed(self):
        if self.is_first:
            try:
                raise SequenceContainsNoElementsError(to_operator_exception(
                    message='',
                    stack=self.stack,
                ))

            except Exception as exc:
                self.observer.on_error(exc)