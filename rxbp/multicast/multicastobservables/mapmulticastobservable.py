from dataclasses import dataclass
from typing import Callable

from rx.disposable import Disposable

from rxbp.multicast.mixins.multicastobservablemixin import MultiCastObservableMixin
from rxbp.multicast.mixins.multicastobservermixin import MultiCastObserverMixin
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.typing import MultiCastValue


@dataclass
class MapMultiCastObservable(MultiCastObservableMixin):
    source: MultiCastObservableMixin
    func: Callable[[MultiCastValue], MultiCastValue]

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        @dataclass
        class FilterMultiCastObserver(MultiCastObserverMixin):
            source: MultiCastObserverMixin
            func: Callable[[MultiCastValue], MultiCastValue]

            def on_next(self, elem: MultiCastValue) -> None:
                try:
                    next = self.func(elem)
                except Exception as exc:
                    self.source.on_error(exc)
                else:
                    self.source.on_next(next)

            def on_error(self, exc: Exception) -> None:
                self.source.on_error(exc)

            def on_completed(self) -> None:
                self.source.on_completed()

        observer = FilterMultiCastObserver(
            source=observer_info.observer,
            func=self.func,
        )

        return self.source.observe(observer_info.copy(observer))
