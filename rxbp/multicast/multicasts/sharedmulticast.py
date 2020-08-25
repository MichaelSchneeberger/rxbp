import threading

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.refcountmulticastobservable import RefCountMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription
from rxbp.multicast.subjects.multicastobservablesubject import MultiCastObservableSubject


class SharedMultiCast(MultiCastMixin):
    def __init__(
            self,
            source: MultiCastMixin,
    ):
        self.source = source

        self._subscription = None
        self._lock = threading.RLock()

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        with self._lock:
            if self._subscription is None:
                subscription = self.source.unsafe_subscribe(subscriber=subscriber)
                subject = MultiCastObservableSubject()
                self._subscription = subscription.copy(
                    observable=RefCountMultiCastObservable(
                        source=subscription.observable,
                        subject=subject,
                        lock=threading.RLock(),
                        count=0,
                    )
                )

        return self._subscription
