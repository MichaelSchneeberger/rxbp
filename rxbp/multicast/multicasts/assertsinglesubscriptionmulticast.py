from dataclasses import dataclass
from traceback import FrameSummary
from typing import List

from rx.internal import SequenceContainsNoElementsError

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription
from rxbp.utils.tooperatorexception import to_operator_exception


@dataclass
class AssertSingleSubscriptionMultiCast(MultiCastMixin):
    source: MultiCastMixin
    stack: List[FrameSummary]
    is_first: bool

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        if not self.is_first:
            raise SequenceContainsNoElementsError(to_operator_exception(
                message='',
                stack=self.stack,
            ))

        self.is_first = False

        subscription = self.source.unsafe_subscribe(subscriber=subscriber)
        return subscription.copy(observable=subscription.observable)
