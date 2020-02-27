from typing import Callable

from rx.internal import SequenceContainsNoElementsError

from rxbp.ack.stopack import stop_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
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

        self.is_first = True

    def observe(self, observer_info: ObserverInfo):
        observer = observer_info.observer

        source = self

        class FirstObserver(Observer):
            def on_next(self, elem: ElementType):
                source.is_first = False
                first_elem = next(iter(elem))

                def gen_first():
                    yield first_elem

                observer.on_next(gen_first())
                observer.on_completed()
                return stop_ack

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                if source.is_first:
                    def func():
                        raise SequenceContainsNoElementsError()

                    try:
                        source.raise_exception(func)
                    except Exception as exc:
                        observer.on_error(exc)

        first_observer = FirstObserver()
        map_subscription = ObserverInfo(first_observer, is_volatile=observer_info.is_volatile)
        return self.source.observe(map_subscription)
