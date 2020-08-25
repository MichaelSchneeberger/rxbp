from typing import Callable

from rx.disposable import CompositeDisposable

from rxbp.flowable import Flowable
from rxbp.multicast.imperative.imperativemulticastbuild import ImperativeMultiCastBuild
from rxbp.multicast.imperative.safeflowablesubject import SafeFlowableSubject
from rxbp.multicast.imperative.safemulticastsubject import SafeMultiCastSubject
from rxbp.multicast.multicast import MultiCast
from rxbp.scheduler import Scheduler


class ImperativeMultiCastBuilder:
    def __init__(
            self,
            composite_disposable: CompositeDisposable,
            scheduler: Scheduler,
    ):
        self.composite_disposable = composite_disposable
        self.scheduler = scheduler

        self.subjects = []

    def create_multicast_subject(self) -> SafeMultiCastSubject:
        subject = SafeMultiCastSubject(
            composite_diposable=self.composite_disposable,
            scheduler=self.scheduler,
        )
        self.subjects.append(subject)
        return subject

    def create_flowable_subject(self) -> SafeFlowableSubject:
        subject = SafeFlowableSubject(
            composite_diposable=self.composite_disposable,
            scheduler=self.scheduler,
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
