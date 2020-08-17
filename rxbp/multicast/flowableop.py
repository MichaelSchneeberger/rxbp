from typing import Iterator

from rxbp.flowable import Flowable
from rxbp.mixins.flowablebasemixin import FlowableBaseMixin
from rxbp.observables.fromiteratorobservable import FromIteratorObservable
from rxbp.selectors.base import Base
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


# todo: what is this class for?
class FlowableOp:
    def from_batches(self, val: Iterator[Iterator], base: Base = None):
        # todo: add assertion about iterator

        class FromIteratorFlowable(FlowableBaseMixin):
            def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
                observable = FromIteratorObservable(
                    iterator=val,
                    scheduler=subscriber.scheduler,
                    subscribe_scheduler=subscriber.subscribe_scheduler,
                )

                return init_subscription(
                    info=BaseAndSelectors(
                        base=base,
                    ),
                    observable=observable,
                )

        return Flowable(FromIteratorFlowable())._share()

    # def from_iterator(self, val: Iterator, base: Base = None):
    #     # todo: add assertion about iterator
    #
    #     return _from_iterator(val=val, base=base)._share()
