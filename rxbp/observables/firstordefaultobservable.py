from typing import Callable, Any

from rxbp.ack.continueack import continue_ack
from rxbp.ack.stopack import stop_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.typing import ElementType


class FirstOrDefaultObservable(Observable):
    def __init__(
            self,
            source: Observable,
            lazy_val: Callable[[], Any],
    ):
        super().__init__()

        self.source = source
        self.lazy_val = lazy_val

        self.is_first = True

    def observe(self, observer_info: ObserverInfo):
        observer = observer_info.observer

        outer_self = self

        class FirstObserver(Observer):
            def on_next(self, elem: ElementType):
                try:
                    first_elem = next(iter(elem))
                except StopIteration:
                    return continue_ack

                outer_self.is_first = False
                observer.on_next([first_elem])
                observer.on_completed()
                return stop_ack

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                if outer_self.is_first:
                    try:
                        observer.on_next(outer_self.lazy_val())
                        observer.on_completed()
                    except Exception as exc:
                        observer.on_error(exc)

        first_observer = FirstObserver()
        map_subscription = ObserverInfo(first_observer, is_volatile=observer_info.is_volatile)
        return self.source.observe(map_subscription)
