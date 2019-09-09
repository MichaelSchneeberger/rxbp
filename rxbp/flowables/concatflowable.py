from typing import Iterable

from rxbp.flowablebase import FlowableBase
from rxbp.observables.concatobservable import ConcatObservable
from rxbp.observables.refcountobservable import RefCountObservable
from rxbp.observablesubjects.observablecacheservefirstsubject import ObservableCacheServeFirstSubject
from rxbp.selectors.bases import ConcatBase
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription, SubscriptionInfo


class ConcatFlowable(FlowableBase):
    def __init__(self, sources: Iterable[FlowableBase]):

        super().__init__()

        self._sources = list(sources)

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        def gen_subscriptions():
            for source in self._sources:
                subscription = source.unsafe_subscribe(subscriber)

                subject = ObservableCacheServeFirstSubject(scheduler=subscriber.scheduler)
                observable = RefCountObservable(source=subscription.observable, subject=subject)

                yield subscription.info, observable

        infos, sources = zip(*gen_subscriptions())

        observable = ConcatObservable(
            sources=sources,
            scheduler=subscriber.scheduler,
            subscribe_scheduler=subscriber.subscribe_scheduler,
        )
        return Subscription(info=SubscriptionInfo(ConcatBase(infos, sources)), observable=observable)
