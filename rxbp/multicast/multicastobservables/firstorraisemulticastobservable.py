from dataclasses import dataclass
from typing import Callable

from rx.disposable import Disposable
from rx.internal import SequenceContainsNoElementsError

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.typing import MultiCastItem


@dataclass
class FilterOrRaiseMultiCastObservable(MultiCastObservable):
    source: MultiCastObservable
    raise_exception: Callable[[Callable[[], None]], None]

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        @dataclass
        class FirstMultiCastObserver(MultiCastObserver):
            source: MultiCastObserver
            raise_exception: Callable[[Callable[[], None]], None]

            def on_next(self, elem: MultiCastItem) -> None:
                observer.on_next(elem)
                observer.on_completed()

            def on_error(self, exc: Exception) -> None:
                self.source.on_error(exc)

            def on_completed(self) -> None:
                def func():
                    raise SequenceContainsNoElementsError()

                try:
                    self.raise_exception(func)
                except Exception as exc:
                    observer.on_error(exc)

        observer = FirstMultiCastObserver(
            source=observer_info.observer,
            raise_exception=self.raise_exception,
        )

        return self.source.observe(observer_info.copy(observer))
