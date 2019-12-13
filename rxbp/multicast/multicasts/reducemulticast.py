from typing import Optional, Union

import rx
import rxbp
import rxbp.observable
from rx import operators as rxop, Observable
from rxbp.flowable import Flowable
from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.multicast.flowables.connectableflowable import ConnectableFlowable
from rxbp.multicast.flowables.flatmapnobackpressureflowable import FlatMapNoBackpressureFlowable
from rxbp.multicast.flowables.flatmergenobackpressureflowable import FlatMergeNoBackpressureFlowable
from rxbp.multicast.flowablestatemixin import FlowableStateMixin
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.rxextensions.liftobservable import LiftObservable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.subscriber import Subscriber


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
                from_state = lambda s: list(s[idx] for idx in range(len(s)))
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
                    # # share only if more than one elem in first state
                    # shared_source = lifted_obs.pipe(
                    #     rxop.share(),
                    # )

                    # lifted_flowable = rxbp.from_rx(lifted_obs)

                    conn_observer = ConnectableObserver(
                        underlying=None,
                        scheduler=info.multicast_scheduler,
                        subscribe_scheduler=info.multicast_scheduler,
                    )

                    # subscribe to source rx.Observables immediately
                    source_flowable = rxbp.from_rx(lifted_obs)
                    subscriber = Subscriber(
                        scheduler=info.multicast_scheduler,
                        subscribe_scheduler=info.multicast_scheduler,
                    )
                    subscription = source_flowable.unsafe_subscribe(subscriber=subscriber)
                    subscription.observable.observe(ObserverInfo(conn_observer))

                    conn_flowable = ConnectableFlowable(conn_observer=conn_observer)

                    if 1 < len(first_state):
                        shared_flowable = RefCountFlowable(conn_flowable)
                    else:
                        shared_flowable = conn_flowable

                    def gen_flowables():
                        for key in first_state.keys():
                            def for_func(key=key, shared_flowable=shared_flowable):
                                def selector(v: FlowableStateMixin):
                                    flowable = to_state(v)[key]
                                    return flowable

                                flattened_flowable = FlatMergeNoBackpressureFlowable(shared_flowable, selector)
                                # flattened_flowable = FlatMapNoBackpressureFlowable(shared_flowable, selector)
                                result = RefCountFlowable(flattened_flowable)

                                return key, Flowable(result)

                            yield for_func()

                    result_flowables = dict(gen_flowables())
                    result = from_state(result_flowables)

                    # def action(_, __):
                    observer.on_next(result)
                    observer.on_completed()

                    # info.multicast_scheduler.schedule(action)

            return ReduceObservable(first=first)

        return LiftObservable(source=source, func=func, subscribe_scheduler=info.multicast_scheduler)
