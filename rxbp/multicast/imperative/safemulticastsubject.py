from dataclasses import dataclass
from typing import Generic

import rx
from rx.disposable import CompositeDisposable
from rx.subject import Subject

from rxbp.multicast.init.initmulticast import init_multicast
from rxbp.multicast.init.initmulticastsubscription import init_multicast_subscription
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription
from rxbp.multicast.subjects.multicastobservablesubject import MultiCastObservableSubject
from rxbp.multicast.typing import MultiCastItem, MultiCastElemType
from rxbp.scheduler import Scheduler


@dataclass
class SafeMultiCastSubject(
    MultiCast[MultiCastElemType],
    MultiCastObserver,
    Generic[MultiCastElemType],
):
    composite_diposable: CompositeDisposable
    subscriber: MultiCastSubscriber
    # multicast_scheduler: Scheduler
    # source_scheduler: Scheduler

    def __post_init__(self):
        self.is_first = True
        self.is_stopped = False
        self.subject = MultiCastObservableSubject()

    @property
    def lift_index(self) -> int:
        return 1

    @property
    def underlying(self) -> MultiCastMixin:
        return self

    @classmethod
    def _copy(cls, underlying: MultiCastMixin):
        return init_multicast(underlying)

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        # assert self.is_first and not self.is_stopped, (
        #     f'subject not initial state when `get_source` is called, '
        #     f'consider to schedule the `on_next` action  with {self.source_scheduler}'
        # )

        return init_multicast_subscription(
            observable=self.subject,
        )

    # def subscribe_to(self, source: MultiCast):
    #     def source_action(_, __):
    #         def multicast_action(_, __):
    #             subscription = source.unsafe_subscribe(MultiCastSubscriber(
    #                 source_scheduler=self.source_scheduler,
    #                 multicast_scheduler=self.multicast_scheduler,
    #             ))
    #
    #             disposable = subscription.observable.observe(MultiCastObserverInfo(
    #                 observer=self,
    #             ))
    #             return disposable
    #             # self.composite_diposable.add(disposable)
    #
    #         return self.multicast_scheduler.schedule(multicast_action)
    #
    #     disposable = self.source_scheduler.schedule(source_action)
    #     self.composite_diposable.add(disposable)

    def on_next(self, val):
        if not self.is_stopped:
            self.is_first = False

            def action():
                try:
                    self.subject.on_next([val])
                except Exception as exc:
                    self.subject.on_error(exc)

            self.subscriber.schedule_action(
                action=action,
                index=1,
            )

    def on_error(self, exc):
        if not self.is_stopped:
            self.is_stopped = True
            self.subject.on_error(exc)

    def on_completed(self):
        if not self.is_stopped:
            self.is_stopped = True

            def action():
                try:
                    self.subject.on_completed()
                except Exception as exc:
                    self.subject.on_error(exc)

            self.subscriber.schedule_action(
                action=action,
                index=1,
            )
