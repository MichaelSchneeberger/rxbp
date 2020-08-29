from abc import ABC
from typing import Callable, Any

import rx
from rx.disposable import Disposable

from rxbp.init.initsubscriber import init_subscriber
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.mixins.sharedflowablemixin import SharedFlowableMixin
from rxbp.mixins.observemixin import ObserveMixin
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.subscription import Subscription


class FlowableSubscribeMixin(
    FlowableMixin,
    ObserveMixin,
    ABC,
):

    def subscribe(
            self,
            on_next: Callable[[Any], None] = None,
            on_error: Callable[[Any], None] = None,
            on_completed: Callable[[], None] = None,
            scheduler: Scheduler = None,
            subscribe_scheduler: Scheduler = None,
            observer: Observer = None,
    ) -> rx.typing.Disposable:
        """ Calling `subscribe` method starts some kind of process that

        start a chain reaction where downsream `Flowables`
        call the `subscribe` method of their linked upstream `Flowable` until
        the sources start emitting data. Once a `Flowable` is subscribed, we
        allow it to have mutable states where it make sense.
        """

        assert isinstance(self, SharedFlowableMixin) is False, \
            'a shared Flowable cannot be subscribed, use Flowable inside MultiCast instead'

        subscribe_scheduler_ = subscribe_scheduler or TrampolineScheduler()
        scheduler_ = scheduler or subscribe_scheduler_

        subscriber = init_subscriber(
            scheduler=scheduler_,
            subscribe_scheduler=subscribe_scheduler_,
        )

        subscription = self.unsafe_subscribe(subscriber=subscriber)

        assert isinstance(subscription, Subscription), \
            f'"{subscription}" must be of type Subscription'

        disposable = self._observe(
            observable=subscription.observable,
            on_next=on_next,
            on_completed=on_completed,
            on_error=on_error,
            observer=observer,
            subscribe_scheduler=subscribe_scheduler_,
        )

        assert isinstance(disposable, rx.typing.Disposable), \
            f'"{disposable}" must be of type Disposable'

        return disposable
