from typing import Optional

import rx
from rx import operators as rxop, Observable

import rxbp
import rxbp.observable

from rxbp.flowable import Flowable
from rxbp.multicast.flowables.flatmapnobackpressureflowable import FlatMapNoBackpressureFlowable
from rxbp.multicast.flowablestatemixin import FlowableStateMixin
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.rxextensions.liftobservable import LiftObservable


class ReduceMultiCast(MultiCastBase):
    def __init__(self, source: MultiCastBase):
        self.source = source

    def get_source(self, info: MultiCastInfo):
        source = self.source.get_source(info=info).pipe(
            rxop.filter(lambda v: isinstance(v, FlowableStateMixin)),
        )

        def func(first: FlowableStateMixin, lifted_obs: Observable):
            class ReduceObservable(Observable):
                def __init__(self, first: FlowableStateMixin):
                    super().__init__()

                    self.first = first

                def _subscribe_core(self,
                        observer: rx.typing.Observer,
                        scheduler: Optional[rx.typing.Scheduler] = None
                        ) -> rx.typing.Disposable:
                    shared_source = lifted_obs.pipe(
                        rxop.share(),
                    )

                    def gen_flowables():
                        for key in first.get_flowable_state().keys():
                            def for_func(key=key):
                                def selector(v: FlowableStateMixin):
                                    flowable = v.get_flowable_state()[key]
                                    return flowable

                                flowable = Flowable(FlatMapNoBackpressureFlowable(rxbp.from_rx(shared_source), selector))

                                return key, flowable

                            yield for_func()

                    conn_flowables = dict(gen_flowables())
                    result = self.first.set_flowable_state(conn_flowables)

                    observer.on_next(result)
                    observer.on_completed()

            return ReduceObservable(first=first)

        return LiftObservable(source=source, func=func, subscribe_scheduler=info.multicast_scheduler).pipe(
            rxop.flat_map(lambda v: v),
        )