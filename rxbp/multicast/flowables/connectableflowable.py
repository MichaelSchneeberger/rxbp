from dataclasses import dataclass

from rx.disposable import Disposable

from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.multicast.observables.connectableobservable import ConnectableObservable
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


@dataclass
class ConnectableFlowable(FlowableMixin):
    conn_observer: ConnectableObserver
    disposable: Disposable

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        observable = ConnectableObservable(
            conn_observer=self.conn_observer,
            disposable=self.disposable,
        )
        return init_subscription(observable=observable)
