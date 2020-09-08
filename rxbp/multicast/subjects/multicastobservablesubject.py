import threading
from typing import List, Optional

from rx.disposable import Disposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo


class MultiCastObservableSubject(MultiCastObservable, MultiCastObserver):
    def __init__(self) -> None:
        super().__init__()

        self.is_disposed = False
        self.observers: List[MultiCastObserver] = []
        self.exception: Optional[Exception] = None

        self.lock = threading.RLock()

    class InnerSubscription(Disposable):
        def __init__(self, subject, observer):
            super().__init__()
            self.subject = subject
            self.observer = observer

            self.lock = threading.RLock()

        def dispose(self) -> None:
            with self.lock:
                if not self.subject.is_disposed and self.observer:
                    if self.observer in self.subject.observers:
                        self.subject.observers.remove(self.observer)
                    self.observer = None

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        with self.lock:
            self.observers.append(observer_info.observer)
            return self.InnerSubscription(self, observer_info.observer)

    def on_next(self, value) -> None:
        if isinstance(value, list):
            materialized_values = value
        else:
            try:
                materialized_values = list(value)
            except Exception as exc:
                self.on_error(exc)
                return

        with self.lock:
            observers = self.observers.copy()

        for observer in observers:
            # try:
            observer.on_next(materialized_values)
            # except Exception as exc:
            #     observer.on_error(exc)

    def on_error(self, error: Exception) -> None:
        with self.lock:
            observers = self.observers.copy()
            self.observers.clear()
            self.exception = error

        for observer in observers:
            observer.on_error(error)

    def on_completed(self) -> None:

        with self.lock:
            observers = self.observers.copy()
            self.observers.clear()

        for observer in observers:
            observer.on_completed()

    def dispose(self) -> None:
        """Unsubscribe all observers and release resources."""

        with self.lock:
            self.is_disposed = True
            self.observers = []
            self.exception = None
