import types
from dataclasses import dataclass
from typing import Callable, Any

from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.typing import ElementType


@dataclass
class DefaultIfEmptyObservable(Observable):
    source: Observable
    lazy_val: Callable[[], Any]

    def observe(self, observer_info: ObserverInfo):
        @dataclass
        class DefaultIfEmptyObserver(Observer):
            source: Observer
            is_first: bool
            lazy_val: Callable[[], Any]

            def on_next(self, elem: ElementType):
                self.is_first = False

                self.on_next = types.MethodType(self.source.on_next, self)  # type: ignore

                return self.source.on_next(elem)

            def on_error(self, exc):
                return self.source.on_error(exc)

            def on_completed(self):
                if self.is_first:
                    try:
                        self.source.on_next([self.lazy_val()])
                        self.source.on_completed()
                    except Exception as exc:
                        self.source.on_error(exc)
                else:
                    self.source.on_completed()

        first_observer = DefaultIfEmptyObserver(
            source=observer_info.observer,
            is_first=True,
            lazy_val=self.lazy_val,
        )
        map_subscription = observer_info.copy(observer=first_observer)
        return self.source.observe(map_subscription)
