import threading
from dataclasses import dataclass
from traceback import FrameSummary
from typing import Callable, List

from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.refcountobservable import RefCountObservable
from rxbp.observablesubjects.cacheservefirstobservablesubject import CacheServeFirstObservableSubject
from rxbp.observablesubjects.observablesubjectbase import ObservableSubjectBase
from rxbp.scheduler import Scheduler
from rxbp.subscriber import Subscriber
from rxbp.utils.tooperatorexception import to_operator_exception


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

        self.start_subscription = False
        self.has_subscription = False
        self.exception = None
        self.lock = threading.RLock()

        self.subscription = None

    def unsafe_subscribe(self, subscriber: Subscriber):
        """ Connects the observable. """

        with self.lock:
            if not self.start_subscription:
                self.start_subscription = True

                try:
                    subscription = self.source.unsafe_subscribe(subscriber)
                except Exception as exc:
                    self.exception = exc
                    raise

                self.has_subscription = True
                subject = self._subject_gen(subscriber.scheduler)

                self.subscription = subscription.copy(
                    observable=RefCountObservable(
                        source=subscription.observable,
                        subject=subject,
                        subscribe_scheduler=subscriber.subscribe_scheduler,
                        stack=self.stack,
                    ),
                )

            else:
                if self.exception is not None:
                    raise self.exception

                if not self.has_subscription:
                    raise Exception(to_operator_exception(
                        message='Looping Flowables is not allowed',
                        stack=self.stack,
                    ))

        return self.subscription
