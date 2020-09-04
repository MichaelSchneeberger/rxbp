from dataclasses import dataclass
from traceback import FrameSummary
from typing import List

from rx.disposable import Disposable, CompositeDisposable

from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.init.initflowable import init_flowable
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.multicast.flowables.connectableflowable import ConnectableFlowable
from rxbp.multicast.flowables.flatconcatnobackpressureflowable import \
    FlatConcatNoBackpressureFlowable
from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.observers.bufferedobserver import BufferedObserver
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.scheduler import Scheduler


@dataclass
class JoinFlowableMultiCastObservable(MultiCastObservable):
    sources: List[MultiCastObservable]
    multicast_scheduler: Scheduler
    source_scheduler: Scheduler
    stack: List[FrameSummary]

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        def gen_conn_flowables():
            for source in self.sources:

                # will be connected as soon as the outgoing Flowable is subscribed
                conn_observer = ConnectableObserver(
                    underlying=None,
                    scheduler=self.multicast_scheduler,
                )

                # MultiCast elements without back-pressure are converted to Flowable elements
                # with back-pressure by buffering the elements if necessary
                observer = BufferedObserver(
                    underlying=conn_observer,
                    scheduler=self.multicast_scheduler,
                    subscribe_scheduler=self.multicast_scheduler,
                    buffer_size=1000,
                )

                disposable = source.observe(MultiCastObserverInfo(
                    observer=observer,
                ))

                # select a single Flowable per MultiCast
                def to_flowable(value):
                    if isinstance(value, FlowableMixin):
                        flowable = value

                    else:
                        raise Exception(f'illegal value "{value}"')

                    return flowable

                flattened_flowable = FlatConcatNoBackpressureFlowable(
                    source=ConnectableFlowable(
                        conn_observer=conn_observer,
                        disposable=disposable,
                    ),
                    selector=to_flowable,
                    subscribe_scheduler=self.source_scheduler,
                )

                # The outgoing Flowables are shared such that they can be subscribed more
                # than once
                yield init_flowable(RefCountFlowable(flattened_flowable, stack=self.stack))

        flowables = list(gen_conn_flowables())

        def action(_, __):
            try:
                observer_info.observer.on_next([flowables])
                observer_info.observer.on_completed()
            except Exception as exc:
                observer_info.observer.on_error(exc)

        # immediately emit a list of flowables representing the joined flowables
        return self.multicast_scheduler.schedule(action)
