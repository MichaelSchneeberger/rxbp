from dataclasses import dataclass
from typing import Generic

import rx
from rx.disposable import CompositeDisposable
from rx.subject import Subject

from rxbp.multicast.liftedmulticast import LiftedMultiCast
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.typing import MultiCastItem
from rxbp.scheduler import Scheduler


@dataclass
class SafeMultiCastSubject(LiftedMultiCast):
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
        self.subject = Subject()

    @classmethod
    def _copy(cls, multi_cast: MultiCastMixin):
        return LiftedMultiCast(multi_cast)

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> rx.typing.Observable[MultiCastItem]:
        assert self.is_first and not self.is_stopped, (
            f'subject not initial state when `get_source` is called, '
            f'consider to schedule the `on_next` action  with {self.scheduler}'
        )

        return self.subject

    def subscribe_to(self, source: MultiCast, scheduler: Scheduler = None):
        scheduler = scheduler or self.scheduler

        observable = source.get_source(MultiCastInfo(
            source_scheduler=scheduler,
            multicast_scheduler=scheduler,
        ))

        disposable = observable.subscribe_(self)

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
