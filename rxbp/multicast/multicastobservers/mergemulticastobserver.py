import threading
from dataclasses import dataclass

from rx.disposable import SingleAssignmentDisposable, CompositeDisposable

from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.typing import MultiCastItem


@dataclass
class MergeMultiCastObserver(MultiCastObserver):
    observer: MultiCastObserver
    lock: threading.RLock()
    inner_subscription: SingleAssignmentDisposable
    group: CompositeDisposable

    def on_next(self, item: MultiCastItem) -> None:
        with self.lock:
            self.observer.on_next(item)

    def on_error(self, exc: Exception) -> None:
        self.observer.on_error(exc)

    def on_completed(self) -> None:
        do_complete = False

        with self.lock:
            self.group.remove(self.inner_subscription)
            if len(self.group) == 0:
                do_complete = True

        if do_complete:
            self.observer.on_completed()