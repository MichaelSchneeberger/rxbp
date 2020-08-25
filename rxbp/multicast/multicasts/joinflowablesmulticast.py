from typing import List

from rx.disposable import Disposable

from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.init.initflowable import init_flowable
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.multicast.flowables.connectableflowable import ConnectableFlowable
from rxbp.multicast.flowables.flatconcatnobackpressureflowable import \
    FlatConcatNoBackpressureFlowable
from rxbp.multicast.init.initmulticastsubscription import init_multicast_subscription
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription
from rxbp.observers.bufferedobserver import BufferedObserver
from rxbp.observers.connectableobserver import ConnectableObserver


class JoinFlowablesMultiCast(MultiCastMixin):
    def __init__(
            self,
            sources: List[MultiCastMixin],
    ):
        self._sources = sources

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        multicast_scheduler = subscriber.multicast_scheduler
        source_scheduler = subscriber.source_scheduler

        def to_flowable(value):
            if isinstance(value, FlowableMixin):
                flowable = value
            elif isinstance(value, list) and len(value) == 1:
                flowable = value[0]
            elif isinstance(value, dict) and len(value) == 1:
                flowable = next(value.values())
            else:
                raise Exception(f'illegal value "{value}"')

            return flowable

        outer_self = self

        class JoinFlowableMultiCastObservable(MultiCastObservable):
            def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
                observer = observer_info.observer

                def gen_conn_flowables():
                    for source in outer_self._sources:
                        conn_observer = ConnectableObserver(
                            underlying=None,
                            scheduler=multicast_scheduler,
                            subscribe_scheduler=multicast_scheduler,
                        )

                        observer = BufferedObserver(
                            underlying=conn_observer,
                            scheduler=multicast_scheduler,
                            subscribe_scheduler=multicast_scheduler,
                            buffer_size=1000,
                        )

                        subscription = source.unsafe_subscribe(MultiCastSubscriber(
                            multicast_scheduler=multicast_scheduler,
                            source_scheduler=multicast_scheduler,
                        ))
                        subscription.observable.observe(MultiCastObserverInfo(
                            observer=observer,
                        ))

                        # # subscribe to source rx.Observables immediately
                        # source_flowable = FromMultiCastToFlowable(source, buffer_size=1000)
                        # subscription = source_flowable.unsafe_subscribe(subscriber=init_subscriber(
                        #     scheduler=multicast_scheduler,
                        #     subscribe_scheduler=multicast_scheduler,
                        # ))
                        # subscription.observable.observe(init_observer_info(conn_observer))

                        conn_flowable = ConnectableFlowable(conn_observer=conn_observer)

                        flattened_flowable = FlatConcatNoBackpressureFlowable(
                            source=conn_flowable,
                            selector=to_flowable,
                            subscribe_scheduler=source_scheduler,
                        )

                        yield init_flowable(RefCountFlowable(flattened_flowable))

                def action(_, __):
                    observer.on_next([flowables])
                    observer.on_completed()

                flowables = list(gen_conn_flowables())

                multicast_scheduler.schedule(action)

        return init_multicast_subscription(
            observable=JoinFlowableMultiCastObservable(),
        )
