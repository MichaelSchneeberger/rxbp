from dataclasses import dataclass
from typing import Callable

from rx.disposable import Disposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.typing import MultiCastItem


@dataclass
class MapMultiCastObservable(MultiCastObservable):
    source: MultiCastObservable
    func: Callable[[MultiCastItem], MultiCastItem]

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        @dataclass
        class MapMultiCastObserver(MultiCastObserver):
            source: MultiCastObserver
            func: Callable[[MultiCastItem], MultiCastItem]

            def on_next(self, elem: MultiCastItem) -> None:
                try:
                    def map_gen():
                        for v in elem:
                            yield self.func(v)

                    next = map_gen()
                except Exception as exc:
                    self.source.on_error(exc)
                else:
                    self.source.on_next(next)

            def on_error(self, exc: Exception) -> None:
                self.source.on_error(exc)

            def on_completed(self) -> None:
                self.source.on_completed()

        observer = MapMultiCastObserver(
            source=observer_info.observer,
            func=self.func,
        )

        return self.source.observe(observer_info.copy(observer))
