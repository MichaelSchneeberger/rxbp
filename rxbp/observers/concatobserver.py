from dataclasses import dataclass
from typing import List, Iterator

from rxbp.acknowledgement.single import Single
from rxbp.observer import Observer
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.typing import ElementType


@dataclass
class ConcatObserver(Observer):
    source: Observer
    connectables: Iterator[ConnectableObserver]

    def __post_init__(self):
        self.ack = None

    def on_next(self, elem: ElementType):
        self.ack = self.source.on_next(elem)
        return self.ack

    def on_error(self, exc):
        self.source.on_error(exc)

        for conn_obs in self.connectables:
            conn_obs.dispose()

    def on_completed(self):
        try:
            connectables = self.connectables

            class _(Single):
                def on_next(self, elem):
                    next_source = next(connectables)
                    next_source.connect()

            if self.ack is None or self.ack.is_sync:
                next_source = next(self.connectables)
                next_source.connect()
            else:
                self.ack.subscribe(_())

        except StopIteration:
            self.source.on_completed()