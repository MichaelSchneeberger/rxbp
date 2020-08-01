from rxbp.ack.continueack import continue_ack
from rxbp.ack.stopack import stop_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.tolistobserver import ToListObserver
from rxbp.typing import ElementType


class ToListObservable(Observable):
    def __init__(self, source: Observable):
        super().__init__()

        self.source = source

    def observe(self, observer_info: ObserverInfo):
        to_list_observer = ToListObserver(
            observer=observer_info.observer
        )
        return self.source.observe(observer_info.copy(
            observer=ToListObserver(
                observer=observer_info.observer
            ),
        ))
