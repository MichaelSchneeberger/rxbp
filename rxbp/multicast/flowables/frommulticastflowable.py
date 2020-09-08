from dataclasses import dataclass

from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.multicast.init.initmulticastsubscriber import init_multicast_subscriber
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.observables.frommulticastobservable import FromMultiCastObservable
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


@dataclass
class FromMultiCastFlowable(FlowableMixin):
    source: MultiCastMixin

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        def gen_subscribe_schedulers():
            yield subscriber.subscribe_scheduler

            for _ in range(self.source.lift_index):
                yield TrampolineScheduler()

        multicast_subscriber = init_multicast_subscriber(
            subscribe_schedulers=tuple(gen_subscribe_schedulers()),
        )

        multicast_subscription = self.source.unsafe_subscribe(
            subscriber=multicast_subscriber,
        )
        source: MultiCastObservable = multicast_subscription.observable

        return init_subscription(
            observable=FromMultiCastObservable(
                source=source,
                subscriber=subscriber,
                multicast_subscriber=multicast_subscriber,
                lift_index=self.source.lift_index,
            ),
        )
