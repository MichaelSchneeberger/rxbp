from typing import Callable

from rx.internal import SequenceContainsNoElementsError

from rxbp.ack.continueack import continue_ack
from rxbp.ack.stopack import stop_ack
from rxbp.observer import Observer
from rxbp.typing import ElementType


class FirstObserver(Observer):
    def __init__(
            self,
            observer: Observer,
            raise_exception: Callable[[Callable[[], None]], None],
    ):
        self.observer = observer
        self.raise_exception = raise_exception

        self.is_first = True

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
            self.is_first = False
            self.observer.on_next([elem_list[0]])
            self.observer.on_completed()
            return stop_ack

        else:
            return continue_ack

    def on_error(self, exc):
        return self.observer.on_error(exc)

    def on_completed(self):
        if self.is_first:
            def func():
                raise SequenceContainsNoElementsError()

            try:
                self.raise_exception(func)
            except Exception as exc:
                self.observer.on_error(exc)