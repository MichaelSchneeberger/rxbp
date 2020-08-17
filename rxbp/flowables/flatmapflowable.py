from typing import Any, Callable

from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablebasemixin import FlowableBaseMixin
from rxbp.observables.flatmapobservable import FlatMapObservable
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class FlatMapFlowable(FlowableBaseMixin):
    def __init__(self, source: FlowableBaseMixin, func: Callable[[Any], FlowableBaseMixin]):
        super().__init__()

        self._source = source
        self._func = func

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)

        def observable_selector(elem: Any):
            flowable = self._func(elem)
            subscription = flowable.unsafe_subscribe(subscriber=subscriber)
            return subscription.observable

        observable = FlatMapObservable(source=subscription.observable, func=observable_selector,
                                       scheduler=subscriber.scheduler, subscribe_scheduler=subscriber.subscribe_scheduler)

        return init_subscription(observable=observable)