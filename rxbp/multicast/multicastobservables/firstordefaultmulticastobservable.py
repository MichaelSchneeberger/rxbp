from dataclasses import dataclass
from typing import Callable, Any

from rx.disposable import Disposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.typing import MultiCastItem


@dataclass
class FilterOrDefaultMultiCastObservable(MultiCastObservable):
    source: MultiCastObservable
    lazy_val: Callable[[], Any]

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        @dataclass
        class FirstOrDefaultMultiCastObserver(MultiCastObserver):
            source: MultiCastObserver
            lazy_val: Callable[[], Any]

            def on_next(self, elem: MultiCastItem) -> None:
                if isinstance(elem, list):
                    if len(elem) == 0:
                        return
                    first_elem = elem[0]

                else:
                    try:
                        first_elem = next(elem)
                    except StopIteration:
                        return

                observer.on_next([first_elem])
                observer.on_completed()

            def on_error(self, exc: Exception) -> None:
                self.source.on_error(exc)

            def on_completed(self) -> None:
                try:
                    observer.on_next([self.lazy_val()])
                    observer.on_completed()
                except Exception as exc:
                    observer.on_error(exc)

        observer = FirstOrDefaultMultiCastObserver(
            source=observer_info.observer,
            lazy_val=self.lazy_val,
        )

        return self.source.observe(observer_info.copy(observer))
