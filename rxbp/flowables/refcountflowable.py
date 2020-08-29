import threading
from traceback import FrameSummary
from typing import Callable, List

from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.refcountobservable import RefCountObservable
from rxbp.observablesubjects.cacheservefirstobservablesubject import CacheServeFirstObservableSubject
from rxbp.observablesubjects.observablesubjectbase import ObservableSubjectBase
from rxbp.scheduler import Scheduler
from rxbp.subscriber import Subscriber


class RefCountFlowable(FlowableMixin):
    """ for internal use
    """

    def __init__(
            self,
            source: FlowableMixin,
            stack: List[FrameSummary],
            subject_gen: Callable[[Scheduler], ObservableSubjectBase] = None,
    ):
        def default_subject_gen(scheduler: Scheduler):
            return CacheServeFirstObservableSubject(scheduler=scheduler)

        self.source = source
        self.stack = stack
        self._subject_gen = subject_gen or default_subject_gen

        self.has_subscription = False
        self.lock = threading.RLock()

        self.subscription = None

    def unsafe_subscribe(self, subscriber: Subscriber):
        """ Connects the observable. """

        with self.lock:
            if not self.has_subscription:
                self.has_subscription = True

                subscription = self.source.unsafe_subscribe(subscriber)
                subject = self._subject_gen(subscriber.scheduler)

                self.subscription = subscription.copy(
                    observable=RefCountObservable(
                        source=subscription.observable,
                        subject=subject,
                        scheduler=subscriber.subscribe_scheduler,
                        stack=self.stack,
                    ),
                )

        return self.subscription
