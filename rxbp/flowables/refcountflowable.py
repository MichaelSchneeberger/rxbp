import threading
from typing import Callable

from rxbp.flowablebase import FlowableBase
from rxbp.observables.refcountobservable import RefCountObservable
from rxbp.scheduler import Scheduler
from rxbp.selectors.bases import ObjectRefBase, Base
from rxbp.observablesubjects.observablepublishsubject import ObservablePublishSubject
from rxbp.observablesubjects.observablesubjectbase import ObservableSubjectBase
from rxbp.subscriber import Subscriber


class RefCountFlowable(FlowableBase):
    """ for internal use
    """

    def __init__(
            self,
            source: FlowableBase,
            subject_gen: Callable[[Scheduler], ObservableSubjectBase] = None,
            base: Base = None
    ):
        base_ = base or source.base or ObjectRefBase(self)  # take over base or create new one
        super().__init__(base=base_, selectable_bases=source.selectable_bases)

        def default_subject_gen(scheduler: Scheduler):
            return ObservablePublishSubject(scheduler=scheduler)

        self.source = source
        self._subject_gen = subject_gen or default_subject_gen

        self.has_subscription = False
        self.source_selectors = None
        self.shared_observable = None
        self.disposable = None
        self.lock = threading.RLock()

    def unsafe_subscribe(self, subscriber: Subscriber):
        """Connects the observable."""
        with self.lock:
            if not self.has_subscription:
                self.has_subscription = True

                source_obs, source_selectors = self.source.unsafe_subscribe(subscriber)
                subject = self._subject_gen(subscriber.scheduler) #PublishSubject(scheduler=subscriber.scheduler)

                self.source_selectors = source_selectors
                self.shared_observable = RefCountObservable(source=source_obs, subject=subject)

        return self.shared_observable, self.source_selectors
