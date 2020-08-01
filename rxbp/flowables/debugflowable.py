from typing import Callable, Any

from rxbp.ack.mixins.ackmixin import AckMixin
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.debugobservable import DebugObservable
from rxbp.observerinfo import ObserverInfo
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


# todo: assert subscribe_scheduler is active
class DebugFlowable(FlowableMixin):
    def __init__(
            self,
            source: FlowableMixin,
            name: str,
            on_next: Callable[[Any], None] = None,
            on_completed: Callable[[], None] = None,
            on_error: Callable[[Exception], None] = None,
            on_ack: Callable[[AckMixin], None] = None,
            on_subscribe: Callable[[ObserverInfo], None] = None,
            on_raw_ack: Callable[[AckMixin], None] = None,
    ):
        super().__init__()

        self._source = source
        self._name = name
        self._on_next = on_next
        self._on_completed = on_completed
        self._on_error = on_error
        self._on_subscribe = on_subscribe
        self._on_ack = on_ack
        self._on_raw_ack = on_raw_ack

    def unsafe_subscribe(self, subscriber: Subscriber):
        print(f'{self._name}.on_subscribe( subscribe_scheduler={subscriber.subscribe_scheduler} )')

        subscription = self._source.unsafe_subscribe(subscriber=subscriber)

        observable = DebugObservable(
            source=subscription.observable,
            name=self._name,
            on_next=self._on_next,
            on_completed=self._on_completed,
            on_error=self._on_error,
            on_subscribe=self._on_subscribe,
            on_ack=self._on_ack,
            on_raw_ack=self._on_raw_ack,
        )

        return init_subscription(info=subscription.info, observable=observable)