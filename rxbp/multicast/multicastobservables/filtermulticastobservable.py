from dataclasses import dataclass
from typing import Callable

from rx.disposable import Disposable

from rxbp.multicast.mixins.multicastobservablemixin import MultiCastObservableMixin
from rxbp.multicast.mixins.multicastobservermixin import MultiCastObserverMixin
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.typing import MultiCastValue


@dataclass
class FilterMultiCastObservable(MultiCastObservableMixin):
    source: MultiCastObservableMixin
    predicate: Callable[[MultiCastValue], bool]

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        @dataclass
        class FilterMultiCastObserver(MultiCastObserverMixin):
            source: MultiCastObserverMixin
            predicate: Callable[[MultiCastValue], bool]

            def on_next(self, elem: MultiCastValue) -> None:
                try:
                    if self.predicate(elem):
                        self.source.on_next(elem)
                except Exception as exc:
                    self.source.on_error(exc)

            def on_error(self, exc: Exception) -> None:
                self.source.on_error(exc)

            def on_completed(self) -> None:
                self.source.on_completed()

        observer = FilterMultiCastObserver(
            source=observer_info.observer,
            predicate=self.predicate,
        )

        return self.source.observe(observer_info.copy(observer))
