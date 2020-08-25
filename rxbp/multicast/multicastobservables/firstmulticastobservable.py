import types
from dataclasses import dataclass
from traceback import FrameSummary
from typing import List

from rx.disposable import Disposable
from rx.internal import SequenceContainsNoElementsError

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.typing import MultiCastItem
from rxbp.utils.tooperatorexception import to_operator_exception


@dataclass
class FirstMultiCastObservable(MultiCastObservable):
    source: MultiCastObservable
    stack: List[FrameSummary]

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        @dataclass
        class FirstMultiCastObserver(MultiCastObserver):
            source: MultiCastObserver
            is_first: bool
            stack: List[FrameSummary]

            def on_next(self, elem: MultiCastItem) -> None:
                if isinstance(elem, list):
                    if len(elem) == 0:
                        return
                    first_elem = elem[0]

                else:
                    try:
                        first_elem = next(elem)
                    except StopIteration:
                        return

                self.is_first = False
                self.source.on_next([first_elem])
                self.source.on_completed()

                self.on_next = types.MethodType(lambda elem: None, self)  # type: ignore

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

        observer = FirstMultiCastObserver(
            source=observer_info.observer,
            is_first=True,
            stack=self.stack,
        )

        return self.source.observe(observer_info.copy(observer))
