from abc import ABC, abstractmethod
from typing import Callable, Any

from rx.disposable import Disposable

from rxbp.init.initsubscriber import init_subscriber
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.mixins.observemixin import ObserveMixin
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class FlowableBaseMixin(ObserveMixin, FlowableMixin, ABC):

    def subscribe(
            self,
            on_next: Callable[[Any], None] = None,
            on_error: Callable[[Any], None] = None,
            on_completed: Callable[[], None] = None,
            scheduler: Scheduler = None,
            subscribe_scheduler: Scheduler = None,
            observer: Observer = None,
    ) -> Disposable:
        """ Calling `subscribe` method starts some kind of process that

        start a chain reaction where downsream `Flowables`
        call the `subscribe` method of their linked upstream `Flowable` until
        the sources start emitting data. Once a `Flowable` is subscribed, we
        allow it to have mutable states where it make sense.
        """

        subscribe_scheduler_ = subscribe_scheduler or TrampolineScheduler()
        scheduler_ = scheduler or subscribe_scheduler_

        subscriber = init_subscriber(
            scheduler=scheduler_,
            subscribe_scheduler=subscribe_scheduler_,
        )

        subscription = self.unsafe_subscribe(subscriber=subscriber)

        assert isinstance(subscription, Subscription), \
            f'"{subscription}" must be of type Subscription'

        return self._observe(
            observable=subscription.observable,
            on_next=on_next,
            on_completed=on_completed,
            on_error=on_error,
            observer=observer,
            subscribe_scheduler=subscribe_scheduler_,
        )

    @abstractmethod
    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        ...
