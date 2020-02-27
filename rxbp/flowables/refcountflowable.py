import threading
from typing import Callable

from rxbp.flowablebase import FlowableBase
from rxbp.observables.refcountobservable import RefCountObservable
from rxbp.observablesubjects.cacheservefirstosubject import CacheServeFirstOSubject
from rxbp.observablesubjects.osubjectbase import OSubjectBase
from rxbp.scheduler import Scheduler
from rxbp.selectors.bases.objectrefbase import ObjectRefBase
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.subscriber import Subscriber


class RefCountFlowable(FlowableBase):
    """ for internal use
    """

    def __init__(
            self,
            source: FlowableBase,
            subject_gen: Callable[[Scheduler], OSubjectBase] = None,
    ):
        # base_ = base or source.base or ObjectRefBase(self)  # take over base or create new one
        # super().__init__(base=base_, selectable_bases=source.selectable_bases)

        super().__init__()

        def default_subject_gen(scheduler: Scheduler):
            return CacheServeFirstOSubject(scheduler=scheduler)

        self.source = source
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

                if subscription.info.base is None:
                    subscription_info = BaseAndSelectors(base=ObjectRefBase(), selectors=subscription.info.selectors)
                else:
                    subscription_info = subscription.info

                shared_observable = RefCountObservable(source=subscription.observable, subject=subject)

                self.subscription = subscription.copy(
                    info=subscription_info,
                    observable=shared_observable,
                )

        return self.subscription
