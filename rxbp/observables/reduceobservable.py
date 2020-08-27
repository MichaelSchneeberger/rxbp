from typing import Callable, Any

from rxbp.acknowledgement.continueack import continue_ack
from rxbp.acknowledgement.stopack import stop_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.typing import ElementType


class ReduceObservable(Observable):
    def __init__(
            self,
            source: Observable,
            func: Callable[[Any, Any], Any],
            initial: Any,
    ):
        super().__init__()

        self.source = source
        self.func = func
        self.initial = initial

    def observe(self, observer_info: ObserverInfo):
        class ToListObserver(Observer):
            def __init__(
                    self,
                    func: Callable[[Any, Any], Any],
                    initial: Any,
            ):
                self.func = func
                self.acc = initial

            def on_next(self, elem: ElementType):
                try:
                    if isinstance(elem, list):
                        materialized_values = elem
                    else:
                        materialized_values = list(elem)

                    for value in materialized_values:
                        self.acc = self.func(self.acc, value)

                except Exception as exc:
                    self.on_error(exc)
                    return stop_ack

                return continue_ack

            def on_error(self, exc):
                return observer_info.observer.on_error(exc)

            def on_completed(self):
                _ = observer_info.observer.on_next([self.acc])
                observer_info.observer.on_completed()

        return self.source.observe(observer_info.copy(
            observer=ToListObserver(
                func=self.func,
                initial=self.initial,
            ),
        ))
