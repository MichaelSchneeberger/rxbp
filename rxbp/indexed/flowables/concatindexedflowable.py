from typing import List

from rxbp.indexed.observables.concatindexedobservable import ConcatIndexedObservable
from rxbp.indexed.selectors.bases.concatbase import ConcatBase
from rxbp.indexed.selectors.flowablebaseandselectors import FlowableBaseAndSelectors
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class ConcatIndexedFlowable(FlowableMixin):
    def __init__(self, sources: List[FlowableMixin]):
        super().__init__()

        self._sources = sources

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        def gen_subscriptions():
            for source in self._sources:
                subscription = source.unsafe_subscribe(subscriber)
                yield subscription

        subscriptions = list(gen_subscriptions())

        observable = ConcatIndexedObservable(
            sources=[s.observable for s in subscriptions],
            scheduler=subscriber.scheduler,
            subscribe_scheduler=subscriber.subscribe_scheduler,
        )

        base = ConcatBase(
            underlying=tuple(s.index for s in subscriptions),
            sources=tuple(observable.selectors),
        )

        return subscriptions[0].copy(
            index=FlowableBaseAndSelectors(base=base),
            observable=observable,
        )
