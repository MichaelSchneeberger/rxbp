from typing import Any

from rxbp.ack import continue_ack, Ack
from rxbp.observer import Observer


class DummyObserver(Observer):
    def on_next(self, v: Any) -> Ack:
        return continue_ack

    def on_error(self, err) -> None:
        pass

    def on_completed(self) -> None:
        pass
