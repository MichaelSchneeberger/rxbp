from typing import Iterable, List

from rxbp.flowablebase import FlowableBase
from rxbp.observables.concatobservable import ConcatObservable
from rxbp.selectors.bases.concatbase import ConcatBase
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class ConcatFlowable(FlowableBase):
    def __init__(self, sources: List[FlowableBase]):
        super().__init__()

        self._sources = sources

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        def gen_subscriptions():
            for source in self._sources:
                subscription = source.unsafe_subscribe(subscriber)
                yield subscription

        subscriptions = list(gen_subscriptions())

        observable = ConcatObservable(
            sources=[s.observable for s in subscriptions],
            scheduler=subscriber.scheduler,
            subscribe_scheduler=subscriber.subscribe_scheduler,
        )

        base = ConcatBase(
            underlying=[s.info for s in subscriptions],
            sources=observable.selectors,
        )

        return Subscription(
            info=BaseAndSelectors(base=base),
            observable=observable,
        )
