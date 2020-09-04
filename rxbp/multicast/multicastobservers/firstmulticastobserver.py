import types
from dataclasses import dataclass
from traceback import FrameSummary
from typing import List

from rx.disposable import SingleAssignmentDisposable
from rx.internal import SequenceContainsNoElementsError

from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.typing import MultiCastItem
from rxbp.utils.tooperatorexception import to_operator_exception


@dataclass
class FirstMultiCastObserver(MultiCastObserver):
    source: MultiCastObserver
    disposable: SingleAssignmentDisposable
    stack: List[FrameSummary]

    def __post_init__(self):
        self.is_first = True

    def on_next(self, item: MultiCastItem) -> None:
        try:
            first_elem = next(iter(item))
        except StopIteration:
            return
        except Exception as exc:
            self.on_error(exc)
            return

        self.is_first = False
        self.source.on_next([first_elem])
        self.source.on_completed()

        self.disposable.dispose()

        # self.on_next = types.MethodType(lambda elem: None, self)  # type: ignore

    def on_error(self, exc: Exception) -> None:
        self.source.on_error(exc)

    def on_completed(self) -> None:
        if self.is_first:
            try:
                raise SequenceContainsNoElementsError(to_operator_exception(
                    message='',
                    stack=self.stack,
                ))

            except Exception as exc:
                self.source.on_error(exc)
