from dataclasses import dataclass
from traceback import FrameSummary
from typing import Callable, List

from rxbp.multicast.init.initmulticastsubscriber import init_multicast_subscriber
from rxbp.multicast.mixins.liftindexmulticastmixin import LiftIndexMultiCastMixin
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.mixins.notliftedmulticastmixin import NotLiftedMultiCastMixin
from rxbp.multicast.multicastobservables.flatmapmulticastobservable import FlatMapMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription
from rxbp.multicast.typing import MultiCastItem
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.utils.tooperatorexception import to_operator_exception


@dataclass
class FlatMapMultiCast(MultiCastMixin):
    source: MultiCastMixin
    func: Callable[[MultiCastItem], MultiCastMixin]
    stack: List[FrameSummary]

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)

        def lifted_func(val):
            multicast = self.func(val)

            if not isinstance(multicast, LiftIndexMultiCastMixin):
                raise Exception(to_operator_exception(
                    message=f'"{multicast}" should be of type MultiCastMixin',
                    stack=self.stack,
                ))

            if isinstance(multicast, NotLiftedMultiCastMixin):
                diff_schedulers = multicast.lift_index + 1 - len(subscriber.subscribe_schedulers)
                if 0 < diff_schedulers:
                    def gen_subscribe_schedulers():
                        for _ in range(diff_schedulers):
                            yield TrampolineScheduler()

                    inner_subscriber = init_multicast_subscriber(
                        subscribe_schedulers=subscriber.subscribe_schedulers + tuple(gen_subscribe_schedulers()),
                    )

                elif diff_schedulers < 0:
                    inner_subscriber = init_multicast_subscriber(
                        subscribe_schedulers=tuple(subscriber.subscribe_schedulers[:multicast.lift_index + 1]),
                    )

                else:
                    inner_subscriber = subscriber

                subscription = multicast.unsafe_subscribe(subscriber=inner_subscriber)

            else:
                inner_subscriber = None
                subscription = multicast.unsafe_subscribe(subscriber=subscriber)

            return subscription.observable, inner_subscriber

        return subscription.copy(
            observable=FlatMapMultiCastObservable(
                source=subscription.observable,
                func=lifted_func,
            )
        )
