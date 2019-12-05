from typing import Optional, Union

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
            rxop.filter(lambda v: isinstance(v, FlowableStateMixin)
                                  or isinstance(v, dict)
                                  or isinstance(v, Flowable)
                                  or isinstance(v, list)),
        )

        def func(first: Union[FlowableStateMixin, dict], lifted_obs: Observable):
            if isinstance(first, dict):
                to_state = lambda s: s
                from_state = lambda s: s
            elif isinstance(first, FlowableStateMixin):
                to_state = lambda s: s.get_flowable_state()
                from_state = lambda s: s.set_flowable_state(s)
            elif isinstance(first, list):
                to_state = lambda l: {idx: elem for idx, elem in enumerate(l)}
                from_state = lambda s: list(s.values())
            elif isinstance(first, Flowable):
                to_state = lambda s: {0: s}
                from_state = lambda s: s[0]
            else:
                raise Exception(f'illegal element "{first}"')

            first_state = to_state(first)

            class ReduceObservable(Observable):
                def __init__(self, first: FlowableStateMixin):
                    super().__init__()

                    self.first = first

                def _subscribe_core(
                        self,
                        observer: rx.typing.Observer,
                        scheduler: Optional[rx.typing.Scheduler] = None
                ) -> rx.typing.Disposable:
                    shared_source = lifted_obs.pipe(
                        rxop.share(),
                    )

                    def gen_flowables():
                        for key in first_state.keys():
                            def for_func(key=key):
                                def selector(v: FlowableStateMixin):
                                    flowable = to_state(v)[key]
                                    return flowable

                                flowable = Flowable(FlatMapNoBackpressureFlowable(rxbp.from_rx(shared_source), selector))

                                return key, flowable

                            yield for_func()

                    conn_flowables = dict(gen_flowables())
                    result = from_state(conn_flowables)

                    observer.on_next(result)
                    observer.on_completed()

            return ReduceObservable(first=first)

        return LiftObservable(source=source, func=func, subscribe_scheduler=info.multicast_scheduler).pipe(
            rxop.flat_map(lambda v: v),
        )