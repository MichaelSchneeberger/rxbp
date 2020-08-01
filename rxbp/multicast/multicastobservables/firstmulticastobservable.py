import types
from dataclasses import dataclass
from typing import Callable

from rx.disposable import Disposable
from rx.internal import SequenceContainsNoElementsError

from rxbp.multicast.mixins.multicastobservablemixin import MultiCastObservableMixin
from rxbp.multicast.mixins.multicastobservermixin import MultiCastObserverMixin
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.typing import MultiCastValue


@dataclass
class FirstMultiCastObservable(MultiCastObservableMixin):
    source: MultiCastObservableMixin

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        @dataclass
        class FirstMultiCastObserver(MultiCastObserverMixin):
            source: MultiCastObserverMixin
            is_first: bool

            def on_next(self, elem: MultiCastValue) -> None:
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
