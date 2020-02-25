from typing import Callable, Any

from rxbp.flowablebase import FlowableBase
from rxbp.observables.doactionobservable import DoActionObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class DoActionFlowable(FlowableBase):
    def __init__(
            self,
            source: FlowableBase,
            on_next: Callable[[Any], None] = None,
            on_completed: Callable[[], None] = None,
            on_error: Callable[[Exception], None] = None,
            on_disposed: Callable[[], None] = None,
    ):
        super().__init__()

        self._source = source
        self.on_next = on_next
        self.on_completed = on_completed
        self.on_error = on_error
        self.on_disposed = on_disposed

    def unsafe_subscribe(self, subscriber: Subscriber):
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)

        observable = DoActionObservable(
            source=subscription.observable,
            on_next=self.on_next,
            on_completed=self.on_completed,
            on_error=self.on_error,
            on_disposed=self.on_disposed,
        )

        return Subscription(info=subscription.info, observable=observable)
