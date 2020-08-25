from rx.disposable import Disposable

from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.multicast.observables.connectableobservable import ConnectableObservable
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class ConnectableFlowable(FlowableMixin):
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
        return init_subscription(observable=observable)
