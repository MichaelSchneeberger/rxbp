from rx import config
from rx.core import Disposable
from rx.internal import DisposedException
from rx.subjects.innersubscription import InnerSubscription

from rxbackpressure.backpressuretypes.syncedbackpressure import SyncedBackpressure, \
    SyncedBackpressureProxy
from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.core.backpressureobserver import BackpressureObserver


class SyncedBackpressureSubject(BackpressureObservable, BackpressureObserver):
    def __init__(self):
        super().__init__()

        self.is_disposed = False
        self.is_stopped = False
        self.observers = []
        self.exception = None

        self.lock = config["concurrency"].RLock()
        self.backpressure = SyncedBackpressure()

    def check_disposed(self):
        if self.is_disposed:
            raise DisposedException()

    def subscribe_backpressure(self, backpressure):
        self.backpressure.add_backpressure(backpressure)

    def _subscribe_core(self, observer):
        with self.lock:
            self.check_disposed()
            if not self.is_stopped:
                self.observers.append(observer)
                proxy_backpressure = SyncedBackpressureProxy(backpressure=self.backpressure)
                self.backpressure.add_proxy(proxy_backpressure)
                observer.subscribe_backpressure(proxy_backpressure)
                return InnerSubscription(self, observer)

            if self.exception:
                observer.on_error(self.exception)
                return Disposable.empty()

            observer.on_completed()
            return Disposable.empty()

    def on_completed(self):
        self._on_completed_core()

    def _on_completed_core(self):
        """Notifies all subscribed observers of the end of the sequence."""

        os = None
        with self.lock:
            self.check_disposed()
            if not self.is_stopped:
                os = self.observers[:]
                self.observers = []
                self.is_stopped = True

        if os:
            for observer in os:
                observer.on_completed()

    def _on_error_core(self, exception):
        """Notifies all subscribed observers with the exception.

        Keyword arguments:
        error -- The exception to send to all subscribed observers.
        """

        os = None
        with self.lock:
            self.check_disposed()
            if not self.is_stopped:
                os = self.observers[:]
                self.observers = []
                self.is_stopped = True
                self.exception = exception

        if os:
            for observer in os:
                observer.on_error(exception)

    def _on_next_core(self, value):
        """Notifies all subscribed observers with the value.

        Keyword arguments:
        value -- The value to send to all subscribed observers.
        """

        os = None
        with self.lock:
            self.check_disposed()
            if not self.is_stopped:
                os = self.observers[:]

        if os:
            for observer in os:
                observer.on_next(value)

    def dispose(self):
        """Unsubscribe all observers and release resources."""

        with self.lock:
            self.is_disposed = True
            self.observers = None