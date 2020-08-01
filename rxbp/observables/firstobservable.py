from typing import Callable

from rx.internal import SequenceContainsNoElementsError

from rxbp.ack.continueack import continue_ack
from rxbp.ack.stopack import stop_ack
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.firstobserver import FirstObserver
from rxbp.typing import ElementType


class FirstObservable(Observable):
    def __init__(
            self,
            source: Observable,
            raise_exception: Callable[[Callable[[], None]], None],
    ):
        super().__init__()

        self.source = source
        self.raise_exception = raise_exception

    def observe(self, observer_info: ObserverInfo):
        next_observer_info = init_observer_info(
            observer=FirstObserver(
                observer=observer_info.observer,
                raise_exception=self.raise_exception,
            ),
            is_volatile=observer_info.is_volatile,
        )

        return self.source.observe(next_observer_info)
