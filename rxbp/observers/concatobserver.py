from dataclasses import dataclass
from typing import List, Iterator

from rxbp.acknowledgement.single import Single
from rxbp.observer import Observer
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.typing import ElementType


@dataclass
class ConcatObserver(Observer):
    next_observer: Observer
    connectables: Iterator[ConnectableObserver]

    def __post_init__(self):
        self.ack = None

    def on_next(self, elem: ElementType):
        self.ack = self.next_observer.on_next(elem)
        return self.ack

    def on_error(self, exc):
        self.next_observer.on_error(exc)

        for conn_obs in self.connectables:
            conn_obs.dispose()

    def on_completed(self):
        try:
            next_source = next(self.connectables)
        except StopIteration:
            self.next_observer.on_completed()
            return

        if self.ack is None or self.ack.is_sync:
            next_source.connect()

        else:
            class ConcatSingle(Single):
                def on_next(self, elem):
                    next_source.connect()

            self.ack.subscribe(ConcatSingle())

