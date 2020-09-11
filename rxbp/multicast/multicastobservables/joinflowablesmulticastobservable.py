from dataclasses import dataclass
from traceback import FrameSummary
from typing import List

from rx.disposable import Disposable

from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.init.initflowable import init_flowable
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.multicast.flowables.connectableflowable import ConnectableFlowable
from rxbp.multicast.flowables.flatconcatnobackpressureflowable import \
    FlatConcatNoBackpressureFlowable
from rxbp.multicast.init.initmulticastobserverinfo import init_multicast_observer_info
from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.observer import Observer
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.scheduler import Scheduler
from rxbp.typing import ElementType


@dataclass
class JoinFlowableMultiCastObservable(MultiCastObservable):
    sources: List[MultiCastObservable]
    subscriber: MultiCastSubscriber
    source_scheduler: Scheduler
    stack: List[FrameSummary]

    def __post_init__(self):
        self.is_sent = False

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        def gen_conn_flowables():
            for source in self.sources:

                # buffers elements received before outgoing Flowable is subscribed
                conn_observer = ConnectableObserver(
                    underlying=None,
                    # scheduler=self.multicast_scheduler,
                )

                outer_self = self

                @dataclass
                class InnerObserver(Observer):
                    underlying: Observer
                    outer_observer: MultiCastObserver

                    def on_next(self, elem: ElementType):
                        if not outer_self.is_sent:

                            outer_self.is_sent = True
                            try:
                                observer_info.observer.on_next([flowables])
                                observer_info.observer.on_completed()
                            except Exception as exc:
                                observer_info.observer.on_error(exc)

                        return self.underlying.on_next(elem)

                    def on_error(self, err):
                        self.outer_observer.on_error(err)
                        self.underlying.on_error(err)

                    def on_completed(self):
                        self.underlying.on_completed()

                disposable = source.observe(
                    observer_info=init_multicast_observer_info(
                        observer=InnerObserver(
                            underlying=conn_observer,
                            outer_observer=observer_info.observer,
                        ),
                    ),
                )

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
                yield init_flowable(RefCountFlowable(
                    source=flattened_flowable,
                    stack=self.stack,
                ))

        flowables = list(gen_conn_flowables())

        def action(_, __):
            if self.is_sent:
                return

            self.is_sent = True
            try:
                observer_info.observer.on_next([flowables])
                observer_info.observer.on_completed()
            except Exception as exc:
                observer_info.observer.on_error(exc)

        disposable = self.subscriber.subscribe_schedulers[1].schedule(action)

        return disposable
