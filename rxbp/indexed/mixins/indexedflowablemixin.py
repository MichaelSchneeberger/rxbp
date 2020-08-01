from abc import ABC, abstractmethod
from typing import Callable, Any

from rx.disposable import Disposable

from rxbp.indexed.indexedsubscription import IndexedSubscription
from rxbp.init.initsubscriber import init_subscriber
from rxbp.mixins.flowabletemplatemixin import FlowableTemplateMixin
from rxbp.mixins.observemixin import ObserveMixin
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.subscriber import Subscriber


class IndexedFlowableMixin(ObserveMixin, FlowableTemplateMixin, ABC):

    def subscribe(
            self,
            on_next: Callable[[Any], None] = None,
            on_error: Callable[[Any], None] = None,
            on_completed: Callable[[], None] = None,
            scheduler: Scheduler = None,
            subscribe_scheduler: Scheduler = None,
            observer: Observer = None,
    ) -> Disposable:

        subscribe_scheduler_ = subscribe_scheduler or TrampolineScheduler()
        scheduler_ = scheduler or subscribe_scheduler_

        subscriber = init_subscriber(
            scheduler=scheduler_,
            subscribe_scheduler=subscribe_scheduler_,
        )

        subscription = self.unsafe_subscribe(subscriber=subscriber)

        assert isinstance(subscription, IndexedSubscription), \
            f'"{subscription}" must be of type IndexedSubscription'

        return self._observe(
            observable=subscription.observable,
            on_next=on_next,
            on_completed=on_completed,
            on_error=on_error,
            observer=observer,
            subscribe_scheduler=subscribe_scheduler_,
        )

    @abstractmethod
    def unsafe_subscribe(self, subscriber: Subscriber) -> IndexedSubscription:
        ...
