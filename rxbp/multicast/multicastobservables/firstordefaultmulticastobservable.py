from dataclasses import dataclass
from typing import Callable, Any

from rx.disposable import Disposable

from rxbp.multicast.mixins.multicastobservablemixin import MultiCastObservableMixin
from rxbp.multicast.mixins.multicastobservermixin import MultiCastObserverMixin
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.typing import MultiCastValue


@dataclass
class FilterOrDefaultMultiCastObservable(MultiCastObservableMixin):
    source: MultiCastObservableMixin
    lazy_val: Callable[[], Any]

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        @dataclass
        class FirstOrDefaultMultiCastObserver(MultiCastObserverMixin):
            source: MultiCastObserverMixin
            lazy_val: Callable[[], Any]

            def on_next(self, elem: MultiCastValue) -> None:
                observer.on_next(x)
                observer.on_completed()

            def on_error(self, exc: Exception) -> None:
                self.source.on_error(exc)

            def on_completed(self) -> None:
                try:
                    observer.on_next(self.lazy_val())
                    observer.on_completed()
                except Exception as exc:
                    observer.on_error(exc)

        observer = FirstOrDefaultMultiCastObserver(
            source=observer_info.observer,
            lazy_val=self.lazy_val,
        )

        return self.source.observe(observer_info.copy(observer))
