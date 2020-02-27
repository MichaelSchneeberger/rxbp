import types
from typing import Callable, Any

from rx.internal import SequenceContainsNoElementsError

from rxbp.ack.stopack import stop_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.typing import ElementType


class DefaultIfEmptyObservable(Observable):
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

        class DefaultIfEmptyObserver(Observer):
            def on_next(self, elem: ElementType):
                outer_self.is_first = False

                self.on_next = types.MethodType(observer.on_next, self)

                return observer.on_next(elem)

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                if outer_self.is_first:
                    try:
                        observer.on_next(outer_self.lazy_val())
                        observer.on_completed()
                    except Exception as exc:
                        observer.on_error(exc)
                else:
                    observer.on_completed()

        first_observer = DefaultIfEmptyObserver()
        map_subscription = ObserverInfo(first_observer, is_volatile=observer_info.is_volatile)
        return self.source.observe(map_subscription)
