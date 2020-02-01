from rx.disposable import Disposable

from rxbp.flowablebase import FlowableBase
from rxbp.multicast.observables.connectableobservable import ConnectableObservable
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class ConnectableFlowable(FlowableBase):
    def __init__(
            self,
            conn_observer: ConnectableObserver,
            disposable: Disposable = None,
    ):
        self._conn_observer = conn_observer
        self._disposable = disposable or Disposable()

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        observable = ConnectableObservable(
            conn_observer=self._conn_observer,
            disposable=self._disposable,
        )
        return Subscription(info=BaseAndSelectors(base=None), observable=observable)
