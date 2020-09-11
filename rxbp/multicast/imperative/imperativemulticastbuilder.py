from dataclasses import dataclass
from typing import Callable

from rx.disposable import CompositeDisposable

from rxbp.flowable import Flowable
from rxbp.multicast.imperative.imperativemulticastbuild import ImperativeMultiCastBuild
from rxbp.multicast.imperative.safeflowablesubject import SafeFlowableSubject
from rxbp.multicast.imperative.safemulticastsubject import SafeMultiCastSubject
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.scheduler import Scheduler


@dataclass
class ImperativeMultiCastBuilder:
    composite_disposable: CompositeDisposable
    subscriber: MultiCastSubscriber
    # source_scheduler: Scheduler
    # multicast_scheduler: Scheduler

    def __post_init__(self):
        self.subjects = []

    def create_multicast_subject(self) -> SafeMultiCastSubject:
        subject = SafeMultiCastSubject(
            composite_diposable=self.composite_disposable,
            subscriber=self.subscriber,
            # source_scheduler=self.source_scheduler,
            # multicast_scheduler=self.multicast_scheduler,
        )
        self.subjects.append(subject)
        return subject

    def create_flowable_subject(self) -> SafeFlowableSubject:
        subject = SafeFlowableSubject(
            composite_diposable=self.composite_disposable,
            scheduler=self.subscriber.subscribe_schedulers[0],
        )
        self.subjects.append(subject)
        return subject

    def return_(
            self,
            blocking_flowable: Flowable,
            output_selector: Callable[[Flowable], MultiCast],
    ):

        return ImperativeMultiCastBuild(
            blocking_flowable=blocking_flowable,
            output_selector=output_selector,
            subjects=self.subjects,
        )
