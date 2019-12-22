import threading
from typing import Callable

from rxbp.flowablebase import FlowableBase
from rxbp.observables.refcountobservable import RefCountObservable
from rxbp.observablesubjects.cacheservefirstosubject import CacheServeFirstOSubject
from rxbp.observablesubjects.osubjectbase import OSubjectBase
from rxbp.scheduler import Scheduler
from rxbp.selectors.bases import ObjectRefBase
from rxbp.subscriber import Subscriber
from rxbp.subscription import SubscriptionInfo, Subscription


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
        self.subscription_info = None
        self.shared_observable = None
        self.disposable = None
        self.lock = threading.RLock()

    def unsafe_subscribe(self, subscriber: Subscriber):
        """Connects the observable."""
        with self.lock:
            if not self.has_subscription:
                self.has_subscription = True

                subscription = self.source.unsafe_subscribe(subscriber)
                subject = self._subject_gen(subscriber.scheduler)

                if subscription.info.base is None:
                    self.subscription_info = SubscriptionInfo(base=ObjectRefBase(), selectors=subscription.info.selectors)
                else:
                    self.subscription_info = subscription.info
                self.shared_observable = RefCountObservable(source=subscription.observable, subject=subject)

        return Subscription(info=self.subscription_info, observable=self.shared_observable)
