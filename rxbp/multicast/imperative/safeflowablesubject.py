from dataclasses import dataclass
from typing import Any, Generic, List

from rx.disposable import CompositeDisposable

from rxbp.acknowledgement.continueack import continue_ack
from rxbp.flowable import Flowable
from rxbp.init.initflowable import init_flowable
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.init.initsubscriber import init_subscriber
from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observablesubjects.cacheservefirstobservablesubject import CacheServeFirstObservableSubject
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.typing import ValueType


@dataclass
class SafeFlowableSubject(
    Flowable[ValueType],
    Observer,
    Generic[ValueType],
):
    composite_diposable: CompositeDisposable
    scheduler: Scheduler

    def __post_init__(self):
        self.is_first = True
        self.is_stopped = False
        self.subscribe_scheduler = TrampolineScheduler()

        self._observable_subject = None

    @property
    def underlying(self) -> FlowableMixin:
        return self

    @property
    def is_hot(self) -> bool:
        return True

    def _copy(self, underlying: FlowableMixin, *args, **kwargs):
        return init_flowable(
            underlying=underlying,
        )

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        # assert self.is_first and not self.is_stopped, (
        #     f'subject not initial state when `subscribe` is called, '
        #     f'consider to schedule the `on_next` action  with {self.scheduler}'
        # )

        if self._observable_subject is None:
            self._observable_subject = CacheServeFirstObservableSubject(scheduler=subscriber.scheduler)
        return init_subscription(observable=self._observable_subject)

    def subscribe_to(self, source: Flowable, scheduler: Scheduler = None):
        scheduler = scheduler or self.scheduler

        subscription = source.unsafe_subscribe(init_subscriber(
            scheduler=scheduler,
            subscribe_scheduler=self.subscribe_scheduler,
        ))

        outer_self = self

        class SafeFlowableObserver(Observer):
            def on_next(self, elem: Any):
                if not outer_self.is_stopped:
                    outer_self.is_first = False

                    if outer_self._observable_subject is not None:
                        return outer_self._observable_subject.on_next(elem)

                return continue_ack

            def on_error(self, exc: Exception):
                outer_self.on_error(exc)

            def on_completed(self):
                outer_self.on_completed()

        observer = SafeFlowableObserver()

        disposable = subscription.observable.observe(init_observer_info(
            observer=observer,
        ))

        self.composite_diposable.add(disposable)

    def on_next(self, elem: Any):
        if not self.is_stopped:
            self.is_first = False

            if self._observable_subject is not None:
                return self._observable_subject.on_next([elem])

        return continue_ack

    def on_next_batch(self, elem: List[Any]):
        if not self.is_stopped:
            self.is_first = False

            if self._observable_subject is not None:
                return self._observable_subject.on_next(elem)

        return continue_ack

    def on_error(self, exc: Exception):
        if not self.is_stopped and self._observable_subject is not None:
            self.is_stopped = True
            self._observable_subject.on_error(exc)

    def on_completed(self):
        if not self.is_stopped and self._observable_subject is not None:
            self.is_stopped = True
            self._observable_subject.on_completed()
