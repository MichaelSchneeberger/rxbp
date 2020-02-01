import sys

from rxbp.ack.continueack import continue_ack
from rxbp.ack.stopack import stop_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.typing import ElementType


class ToListObservable(Observable):
    def __init__(self, source: Observable):
        super().__init__()

        self.source = source

    def observe(self, observer_info: ObserverInfo):
        observer = observer_info.observer

        class ToListObserver(Observer):
            def __init__(self):
                self.queue = []

            def on_next(self, elem: ElementType):
                if isinstance(elem, list):
                    materialized_values = elem
                else:
                    try:
                        materialized_values = list(elem)
                    except Exception as exc:
                        self.on_error(exc)
                        return stop_ack

                self.queue += materialized_values

                return continue_ack

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                _ = observer.on_next([self.queue])
                observer.on_completed()

        to_list_observer = ToListObserver()
        return self.source.observe(observer_info.copy(observer=to_list_observer))
