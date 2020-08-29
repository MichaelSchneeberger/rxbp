from dataclasses import dataclass

from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobservables.toflowablemulticastobservable import FromMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


@dataclass
class FromMultiCastFlowable(FlowableMixin):
    source: MultiCastMixin

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        multicast_subscribe_scheduler = TrampolineScheduler()

        multicast_subscription = self.source.unsafe_subscribe(
            subscriber=MultiCastSubscriber(
                source_scheduler=subscriber.subscribe_scheduler,
                multicast_scheduler=multicast_subscribe_scheduler,
            )
        )
        source: MultiCastObservable = multicast_subscription.observable

        return init_subscription(
            observable=FromMultiCastObservable(
                source=source,
                subscriber=subscriber,
                multicast_subscribe_scheduler=multicast_subscribe_scheduler,
            ),
        )