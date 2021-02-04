from rx.core import typing

from rxbp.init.initsubscription import init_subscription
from rxbp.internal.timer import _interval
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.intervalobservable import IntervalObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class IntervalFlowable(FlowableMixin):
    def __init__(
            self,
            period: typing.RelativeTime,
    ):
        self.period = period

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        return init_subscription(
            observable=IntervalObservable(
                source=_interval(self.period),
                scheduler=subscriber.scheduler,
                subscribe_scheduler=subscriber.subscribe_scheduler,
            )
        )
