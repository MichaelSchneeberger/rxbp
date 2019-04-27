import threading

from rxbp.flowablebase import FlowableBase
from rxbp.observables.refcountobservable import RefCountObservable
from rxbp.subjects.publishsubject import PublishSubject
from rxbp.subscriber import Subscriber


class RefCountFlowable(FlowableBase):
    """
    source.share(3, lambda o1, o2, o3:
        return o1.zip(o2).zip(o3))
    """

    def __init__(self, source: FlowableBase): #, base: Any = None, selectable_bases: Set[Any] = None):
        base = source.base or self  # take over base or create new one
        super().__init__(base=base, selectable_bases=source.selectable_bases)

        self.source = source

        self.has_subscription = False
        self.source_selectors = None
        self.shared_observable = None
        self.disposable = None
        self.lock = threading.RLock()
        self.base = source.base or source  # take over base or create new one

    def unsafe_subscribe(self, subscriber: Subscriber):
        """Connects the observable."""

        with self.lock:
            if not self.has_subscription:
                source_obs, source_selectors = self.source.unsafe_subscribe(subscriber)
                subject = PublishSubject(scheduler=subscriber.scheduler)

                self.has_subscription = True
                self.source_selectors = source_selectors
                self.shared_observable = RefCountObservable(source=source_obs, subject=subject)

        return self.shared_observable, self.source_selectors
