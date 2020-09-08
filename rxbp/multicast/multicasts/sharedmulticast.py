import threading
from dataclasses import dataclass
from traceback import FrameSummary
from typing import List

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.refcountmulticastobservable import RefCountMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription
from rxbp.multicast.subjects.multicastobservablesubject import MultiCastObservableSubject


@dataclass
class SharedMultiCast(MultiCastMixin):
    source: MultiCastMixin
    stack: List[FrameSummary]

    def __post_init__(self):
        self.subscription = None
        self.lock = threading.RLock()

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        with self.lock:
            if self.subscription is None:
                multicast_scheduler = subscriber.subscribe_schedulers[-1]
                subscription = self.source.unsafe_subscribe(subscriber=subscriber)
                self.subscription = subscription.copy(
                    observable=RefCountMultiCastObservable(
                        source=subscription.observable,
                        subject=MultiCastObservableSubject(),
                        multicast_scheduler=multicast_scheduler,
                        stack=self.stack,
                    )
                )

        return self.subscription
