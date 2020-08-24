from dataclasses import dataclass
from typing import Callable

from rx.disposable import Disposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.typing import MultiCastItem


@dataclass
class FilterMultiCastObservable(MultiCastObservable):
    source: MultiCastObservable
    predicate: Callable[[MultiCastItem], bool]

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        @dataclass
        class FilterMultiCastObserver(MultiCastObserver):
            source: MultiCastObserver
            predicate: Callable[[MultiCastItem], bool]

            def on_next(self, elem: MultiCastItem) -> None:
                def gen_filtered_iterable():
                    for e in elem:
                        if self.predicate(e):
                            yield e

                return self.source.on_next(gen_filtered_iterable())

            def on_error(self, exc: Exception) -> None:
                self.source.on_error(exc)

            def on_completed(self) -> None:
                self.source.on_completed()

        observer = FilterMultiCastObserver(
            source=observer_info.observer,
            predicate=self.predicate,
        )

        return self.source.observe(observer_info.copy(observer))
