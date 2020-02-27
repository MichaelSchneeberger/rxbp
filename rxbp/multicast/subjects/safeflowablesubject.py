from typing import Any, Generic

from rx.disposable import CompositeDisposable

from rxbp.ack.continueack import continue_ack
from rxbp.flowable import Flowable
from rxbp.flowablebase import FlowableBase
from rxbp.multicast.multicastflowable import MultiCastFlowable
from rxbp.observablesubjects.cacheservefirstosubject import CacheServeFirstOSubject
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.typing import ValueType


class SafeFlowableSubject(MultiCastFlowable[ValueType], Observer, Generic[ValueType]):
    def __init__(
            self,
            composite_diposable: CompositeDisposable,
            scheduler: Scheduler,
    ):
        super().__init__(underlying=self)

        self.composite_diposable = composite_diposable
        self.scheduler = scheduler

        self.is_first = True
        self.is_stopped = False
        self.subscribe_scheduler = TrampolineScheduler()

        self._observable_subject = None

    def _copy(cls, flowable: FlowableBase):
        return MultiCastFlowable(flowable)

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        assert self.is_first and not self.is_stopped, (
            f'subject not initial state when `subscribe` is called, '
            f'consider to schedule the `on_next` action  with {self.scheduler}'
        )

        if self._observable_subject is None:
            self._observable_subject = CacheServeFirstOSubject(scheduler=subscriber.scheduler)
        return Subscription(BaseAndSelectors(base=None), self._observable_subject)

    def subscribe_to(self, source: Flowable, scheduler: Scheduler = None):
        scheduler = scheduler or self.scheduler

        subscription = source.unsafe_subscribe(Subscriber(
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

        disposable = subscription.observable.observe(ObserverInfo(
            observer=observer,
        ))

        self.composite_diposable.add(disposable)

    def on_next(self, elem: Any):
        if not self.is_stopped:
            self.is_first = False

            if self._observable_subject is not None:
                return self._observable_subject.on_next([elem])

        return continue_ack

    def on_error(self, exc: Exception):
        if not self.is_stopped and self._observable_subject is not None:
            self.is_stopped = True
            self._observable_subject.on_error(exc)

    def on_completed(self):
        if not self.is_stopped and self._observable_subject is not None:
            self.is_stopped = True
            self._observable_subject.on_completed()
