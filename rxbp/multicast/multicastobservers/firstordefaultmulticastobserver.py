import types
from dataclasses import dataclass
from typing import Callable, Any

from rx.disposable import SingleAssignmentDisposable

from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.typing import MultiCastItem


@dataclass
class FirstOrDefaultMultiCastObserver(MultiCastObserver):
    source: MultiCastObserver
    disposable: SingleAssignmentDisposable
    lazy_val: Callable[[], Any]

    def __post_init__(self):
        self.is_first = True

    def on_next(self, item: MultiCastItem) -> None:
        try:
            first_elem = next(iter(item))
        except StopIteration:
            return
        except Exception as exc:
            self.on_error(exc)
            return

        self.is_first = False
        self.source.on_next([first_elem])
        self.source.on_completed()

        self.disposable.dispose()
        # self.on_next = types.MethodType(lambda elem: None, self)  # type: ignore

    def on_error(self, exc: Exception) -> None:
        self.source.on_error(exc)

    def on_completed(self) -> None:
        if self.is_first:
            try:
                self.source.on_next([self.lazy_val()])
                self.source.on_completed()
            except Exception as exc:
                self.source.on_error(exc)