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
    def __init__(
            self,
            composite_diposable: CompositeDisposable,
            scheduler: Scheduler,
    ):
        super().__init__()

        self.composite_diposable = composite_diposable
        self.scheduler = scheduler

        self.is_first = True
        self.is_stopped = False
        self.subject = MultiCastObservableSubject()

    @property
    def nested_layer(self) -> int:
        return 0

    @property
    def underlying(self) -> MultiCastMixin:
        return self

    @property
    def is_hot_on_subscribe(self) -> bool:
        return True

    @classmethod
    def _copy(cls, multi_cast: MultiCastMixin):
        return init_multicast(multi_cast)

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        assert self.is_first and not self.is_stopped, (
            f'subject not initial state when `get_source` is called, '
            f'consider to schedule the `on_next` action  with {self.scheduler}'
        )

        return init_multicast_subscription(
            observable=self.subject,
        )

    def subscribe_to(self, source: MultiCast, scheduler: Scheduler = None):
        scheduler = scheduler or self.scheduler

        subscription = source.unsafe_subscribe(MultiCastSubscriber(
            source_scheduler=scheduler,
            multicast_scheduler=scheduler,
        ))

        disposable = subscription.observable.observe(MultiCastObserverInfo(
            observer=self,
        ))

        self.composite_diposable.add(disposable)

    def on_next(self, val):
        if not self.is_stopped:
            self.is_first = False
            self.subject.on_next(val)

    def on_error(self, exc):
        if not self.is_stopped:
            self.is_stopped = True
            self.subject.on_error(exc)

    def on_completed(self):
        if not self.is_stopped:
            self.is_stopped = True
            self.subject.on_completed()
