import types
from dataclasses import dataclass

from rx.disposable import Disposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.typing import MultiCastItem


@dataclass
class FirstMultiCastObservable(MultiCastObservable):
    source: MultiCastObservable

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        @dataclass
        class FirstMultiCastObserver(MultiCastObserver):
            source: MultiCastObserver
            is_first: bool

            def on_next(self, elem: MultiCastItem) -> None:
                self.is_first = False
                self.source.on_next(elem)
                self.source.on_completed()

                self.on_next = types.MethodType(lambda elem: None, self)  # type: ignore

            def on_error(self, exc: Exception) -> None:
                self.source.on_error(exc)

            def on_completed(self) -> None:
                if self.is_first:
                    self.source.on_completed()

        observer = FirstMultiCastObserver(
            source=observer_info.observer,
            is_first=True,
        )

        return self.source.observe(observer_info.copy(observer))
