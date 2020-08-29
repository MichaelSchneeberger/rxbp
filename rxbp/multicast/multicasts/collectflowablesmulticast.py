from dataclasses import dataclass
from traceback import FrameSummary
from typing import Any, List

from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.init.initflowable import init_flowable
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.multicast.flowables.connectableflowable import ConnectableFlowable
from rxbp.multicast.flowables.flatconcatnobackpressureflowable import \
    FlatConcatNoBackpressureFlowable
from rxbp.multicast.flowables.flatmergenobackpressureflowable import \
    FlatMergeNoBackpressureFlowable
from rxbp.multicast.mixins.flowablestatemixin import FlowableStateMixin
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobservables.liftedmulticastobservable import LiftedMultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription
from rxbp.observers.bufferedobserver import BufferedObserver
from rxbp.observers.connectableobserver import ConnectableObserver


@dataclass
class CollectFlowablesMultiCast(MultiCastMixin):
    source: MultiCastMixin
    maintain_order: bool
    stack: List[FrameSummary]

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        multicast_scheduler = subscriber.multicast_scheduler
        source_scheduler = subscriber.source_scheduler

        subscription = self.source.unsafe_subscribe(subscriber=subscriber)

        def func(lifted_obs: MultiCastObservable, first: Any):
            if isinstance(first, dict):
                to_state = lambda s: s
                from_state = lambda s: s

            elif isinstance(first, FlowableStateMixin):
                to_state = lambda s: s.get_flowable_state()
                from_state = lambda s: s.set_flowable_state(s)

            elif isinstance(first, list):
                to_state = lambda l: {idx: elem for idx, elem in enumerate(l)}
                from_state = lambda s: list(s[idx] for idx in range(len(s)))

            elif isinstance(first, FlowableMixin):
                to_state = lambda s: {0: s}
                from_state = lambda s: s[0]

            else:
                to_state = lambda s: s
                from_state = lambda s: s

            first_state = to_state(first)

            conn_observer = ConnectableObserver(
                underlying=None,
                scheduler=multicast_scheduler,
                subscribe_scheduler=multicast_scheduler,
            )
            conn_flowable = ConnectableFlowable(conn_observer=conn_observer)

            observer = BufferedObserver(
                underlying=conn_observer,
                scheduler=multicast_scheduler,
                subscribe_scheduler=multicast_scheduler,
                buffer_size=1000,
            )

            # def action(_, __):
            lifted_obs.observe(MultiCastObserverInfo(observer=observer))

            # multicast_scheduler.schedule(action)

            # # subscribe to source MultiCast immediately
            # source_flowable = FromMultiCastToFlowable(lifted_obs, buffer_size=1000)
            # subscription = source_flowable.unsafe_subscribe(subscriber=init_subscriber(
            #     scheduler=multicast_scheduler,
            #     subscribe_scheduler=multicast_scheduler,
            # ))
            # subscription.observable.observe(init_observer_info(
            #     observer=conn_observer,
            # ))

            if 1 < len(first_state):
                shared_flowable = RefCountFlowable(conn_flowable, stack=self.stack)
            else:
                shared_flowable = conn_flowable

            def gen_flowables():
                for key in first_state.keys():
                    def selector(v: FlowableStateMixin, key=key):
                        flowable = to_state(v)[key]
                        return flowable

                    if self.maintain_order:
                        flattened_flowable = FlatConcatNoBackpressureFlowable(
                            source=shared_flowable,
                            selector=selector,
                            subscribe_scheduler=source_scheduler,
                        )
                    else:
                        flattened_flowable = FlatMergeNoBackpressureFlowable(
                            source=shared_flowable,
                            selector=selector,
                            subscribe_scheduler=source_scheduler,
                        )

                    flowable = init_flowable(RefCountFlowable(flattened_flowable, stack=self.stack))
                    yield key, flowable

            return from_state(dict(gen_flowables()))

        return subscription.copy(observable=LiftedMultiCastObservable(
            source=subscription.observable,
            func=func,
            scheduler=subscriber.multicast_scheduler,
        ))
