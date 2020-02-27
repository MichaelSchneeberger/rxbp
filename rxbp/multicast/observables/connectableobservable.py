from rx.core.typing import Disposable

from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.connectableobserver import ConnectableObserver


class ConnectableObservable(Observable):
    def __init__(
            self,
            conn_observer: ConnectableObserver,
            disposable: Disposable,
    ):
        self._conn_observer = conn_observer
        self._disposable = disposable

    def observe(self, observer_info: ObserverInfo) -> Disposable:
        self._conn_observer.underlying = observer_info.observer
        self._conn_observer.connect()
        return self._disposable
