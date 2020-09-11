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
class LastObserver(Observer):
    observer: Observer
    stack: List[FrameSummary]

    def __post_init__(self):
        self.has_value = False
        self.last_value = None

    def on_next(self, elem: ElementType):

        if isinstance(elem, list):
            materialized = elem
        else:
            materialized = list(elem)

        if 0 < len(materialized):
            self.has_value = True
            self.last_value = materialized[-1]

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
