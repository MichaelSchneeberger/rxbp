from typing import List

import rx
from rx import Observable

from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.multicast.flowables.connectableflowable import ConnectableFlowable
from rxbp.multicast.flowables.flatmapnobackpressureflowable import FlatMapNoBackpressureFlowable
from rxbp.multicast.multicast import MultiCast
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.source import from_rx
from rxbp.flowable import Flowable
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.typing import MultiCastValue
from rxbp.subscriber import Subscriber


class ZipMultiCast(MultiCastBase):
    def __init__(
            self,
            sources: List[MultiCast],
    ):
        self._sources = sources

    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable[MultiCastValue]:
        def to_flowable(value):
            if isinstance(value, Flowable):
                flowable = value
            elif isinstance(value, list) and len(value) == 1:
                flowable = value[0]
            elif isinstance(value, dict) and len(value) == 1:
                flowable = next(value.values())
            else:
                raise Exception(f'illegal value "{value}"')

            return flowable

        def subscribe(observer, scheduler=None):

            def gen_conn_flowables():
                for source in self._sources:
                    def for_func(source=source):
                        conn_observer = ConnectableObserver(
                            underlying=None,
                            scheduler=info.multicast_scheduler,
                            subscribe_scheduler=info.multicast_scheduler,
                        )

                        # subscribe to source rx.Observables immediately
                        source_flowable = from_rx(source.get_source(info))
                        subscriber = Subscriber(
                            scheduler=info.multicast_scheduler,
                            subscribe_scheduler=info.multicast_scheduler,
                        )
                        subscription = source_flowable.unsafe_subscribe(subscriber=subscriber)
                        subscription.observable.observe(ObserverInfo(conn_observer))

                        conn_flowable = ConnectableFlowable(conn_observer=conn_observer)

                        flattened_flowable = FlatMapNoBackpressureFlowable(conn_flowable, to_flowable)

                        ref_count_flowable = RefCountFlowable(flattened_flowable)

                        return Flowable(ref_count_flowable)

                    yield for_func()

            def action(_, __):
                observer.on_next(flowables)
                observer.on_completed()

            info.multicast_scheduler.schedule(action)

            flowables = list(gen_conn_flowables())

        return Observable(subscribe=subscribe)
