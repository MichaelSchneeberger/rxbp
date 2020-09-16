from dataclasses import dataclass
from traceback import FrameSummary
from typing import List

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.collectflowablesmulticastobservable import CollectFlowablesMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription


@dataclass
class CollectFlowablesMultiCast(MultiCastMixin):
    """
    On next and completed
    ---------------------

    Before first element received
    O----->O----->O
    1      2      3

    After first element received
    O----->O----->o----->o----->o----->o
    1      2      4      5      6      7

    1. previous multicast observer
    2. CollectFlowablesMultiCastObserver
    3. next multicast observer
    4. ConnectableObserver
    5. RefCountObserver
    6. FlatNoBackpressureObserver
    7. next flowable observer

    On error
    --------

    Before first element received
    O----->O----->O
    1      2      3

    After first element received
             *--->o
            /     3
    O----->O----->o----->o----->o----->o
    1      2      4      5      6      7


    """

    source: MultiCastMixin
    maintain_order: bool
    stack: List[FrameSummary]

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)

        return subscription.copy(
            observable=CollectFlowablesMultiCastObservable(
                source=subscription.observable,
                maintain_order=self.maintain_order,
                stack=self.stack,
                subscriber=subscriber,
            )
        )
