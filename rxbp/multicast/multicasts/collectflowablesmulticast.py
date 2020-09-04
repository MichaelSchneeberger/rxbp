import threading
import types
from dataclasses import dataclass
from itertools import chain
from traceback import FrameSummary
from typing import Any, List

import rx
from rx.disposable import CompositeDisposable

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
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription
from rxbp.multicast.observer.flatconcatnobackpressureobserver import FlatConcatNoBackpressureObserver
from rxbp.multicast.typing import MultiCastItem
from rxbp.observer import Observer
from rxbp.observers.bufferedobserver import BufferedObserver
from rxbp.observers.connectableobserver import ConnectableObserver


@dataclass
class CollectFlowablesMultiCast(MultiCastMixin):
    source: MultiCastMixin
    maintain_order: bool
    stack: List[FrameSummary]

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)

        @dataclass
        class CollectFlowablesMultiCastObserver(MultiCastObserver):
            next_observer: MultiCastObserver
            disposable: rx.typing.Disposable
            maintain_order: bool
            stack: List[FrameSummary]

            def __post_init__(self):
                self.is_first = True
                self.buffered_observer: Observer = None

            def on_next(self, item: MultiCastItem) -> None:
                if isinstance(item, list):
                    if len(item) == 0:
                        return

                    first_elem = item[0]
                    all_elem = item

                else:
                    try:
                        first_elem = next(item)
                        all_elem = chain([first_elem], item)
                    except StopIteration:
                        return

                if isinstance(first_elem, dict):
                    to_state = lambda s: s
                    from_state = lambda s: s

                elif isinstance(first_elem, FlowableStateMixin):
                    to_state = lambda s: s.get_flowable_state()
                    from_state = lambda s: s.set_flowable_state(s)

                elif isinstance(first_elem, list):
                    to_state = lambda l: {idx: elem for idx, elem in enumerate(l)}
                    from_state = lambda s: list(s[idx] for idx in range(len(s)))

                elif isinstance(first_elem, FlowableMixin):
                    to_state = lambda s: {0: s}
                    from_state = lambda s: s[0]

                else:
                    to_state = lambda s: s
                    from_state = lambda s: s

                first_state = to_state(first_elem)

                # UpstreamObserver --> BufferedObserver --> ConnectableObserver --> RefCountObserver --> FlatNoBackpressureObserver

                conn_observer = ConnectableObserver(
                    underlying=None,
                    scheduler=subscriber.multicast_scheduler,
                )

                buffered_observer = BufferedObserver(
                    underlying=conn_observer,
                    scheduler=subscriber.multicast_scheduler,
                    subscribe_scheduler=subscriber.multicast_scheduler,
                    buffer_size=1000,
                )

                self.buffered_observer = buffered_observer

                conn_flowable = ConnectableFlowable(
                    conn_observer=conn_observer,
                    disposable=self.disposable,
                )

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
                                subscribe_scheduler=subscriber.source_scheduler,
                            )
                        else:
                            flattened_flowable = FlatMergeNoBackpressureFlowable(
                                source=shared_flowable,
                                selector=selector,
                                subscribe_scheduler=subscriber.source_scheduler,
                            )

                        flowable = init_flowable(RefCountFlowable(flattened_flowable, stack=self.stack))
                        yield key, flowable

                collected_item = from_state(dict(gen_flowables()))
                self.next_observer.on_next([collected_item])
                self.next_observer.on_completed()

                self.is_first = False

                # observable didn't get subscribed
                if conn_observer.underlying is None:
                    def on_next_if_not_subscribed(self, val):
                        pass

                    self.on_next = types.MethodType(on_next_if_not_subscribed, self)

                    # if there is no inner subscriber, dispose the source
                    self.disposable.dispose()
                    return

                _ = buffered_observer.on_next(all_elem)

                def on_next_after_first(self, elem: MultiCastItem):
                    _ = buffered_observer.on_next(elem)

                self.on_next = types.MethodType(on_next_after_first, self)  # type: ignore

            def on_error(self, exc: Exception) -> None:
                if self.is_first:
                    self.next_observer.on_error(exc)
                else:
                    self.buffered_observer.on_error(exc)

            def on_completed(self) -> None:
                if self.is_first:
                    self.next_observer.on_completed()
                else:
                    self.buffered_observer.on_completed()

        @dataclass
        class CollectFlowablesMultiCastObservable(MultiCastObservable):
            source: MultiCastObservable
            maintain_order: bool
            stack: List[FrameSummary]

            def observe(self, observer_info: MultiCastObserverInfo) -> rx.typing.Disposable:
                composite_disposable = CompositeDisposable()

                disposable = self.source.observe(observer_info.copy(
                    observer=CollectFlowablesMultiCastObserver(
                        next_observer=observer_info.observer,
                        disposable=composite_disposable,
                        maintain_order=self.maintain_order,
                        stack=self.stack,
                    ),
                ))
                composite_disposable.add(disposable)

                return composite_disposable

        return subscription.copy(
            observable=CollectFlowablesMultiCastObservable(
                source=subscription.observable,
                maintain_order=self.maintain_order,
                stack=self.stack,
            )
        )

        # def func(lifted_obs: MultiCastObservable, first: Any):
        #     if isinstance(first, dict):
        #         to_state = lambda s: s
        #         from_state = lambda s: s
        #
        #     elif isinstance(first, FlowableStateMixin):
        #         to_state = lambda s: s.get_flowable_state()
        #         from_state = lambda s: s.set_flowable_state(s)
        #
        #     elif isinstance(first, list):
        #         to_state = lambda l: {idx: elem for idx, elem in enumerate(l)}
        #         from_state = lambda s: list(s[idx] for idx in range(len(s)))
        #
        #     elif isinstance(first, FlowableMixin):
        #         to_state = lambda s: {0: s}
        #         from_state = lambda s: s[0]
        #
        #     else:
        #         to_state = lambda s: s
        #         from_state = lambda s: s
        #
        #     first_state = to_state(first)
        #
        #     conn_observer = ConnectableObserver(
        #         underlying=None,
        #         scheduler=subscriber.multicast_scheduler,
        #         # subscribe_scheduler=multicast_scheduler,
        #     )
        #
        #     observer = BufferedObserver(
        #         underlying=conn_observer,
        #         scheduler=subscriber.multicast_scheduler,
        #         subscribe_scheduler=subscriber.multicast_scheduler,
        #         buffer_size=1000,
        #     )
        #
        #     # def action(_, __):
        #     disposable = lifted_obs.observe(MultiCastObserverInfo(observer=observer))
        #
        #     conn_flowable = ConnectableFlowable(
        #         conn_observer=conn_observer,
        #         disposable=disposable,
        #     )
        #
        #     # multicast_scheduler.schedule(action)
        #
        #     # # subscribe to source MultiCast immediately
        #     # source_flowable = FromMultiCastToFlowable(lifted_obs, buffer_size=1000)
        #     # subscription = source_flowable.unsafe_subscribe(subscriber=init_subscriber(
        #     #     scheduler=multicast_scheduler,
        #     #     subscribe_scheduler=multicast_scheduler,
        #     # ))
        #     # subscription.observable.observe(init_observer_info(
        #     #     observer=conn_observer,
        #     # ))
        #
        #     if 1 < len(first_state):
        #         shared_flowable = RefCountFlowable(conn_flowable, stack=self.stack)
        #     else:
        #         shared_flowable = conn_flowable
        #
        #     def gen_flowables():
        #         for key in first_state.keys():
        #             def selector(v: FlowableStateMixin, key=key):
        #                 flowable = to_state(v)[key]
        #                 return flowable
        #
        #             if self.maintain_order:
        #                 flattened_flowable = FlatConcatNoBackpressureFlowable(
        #                     source=shared_flowable,
        #                     selector=selector,
        #                     subscribe_scheduler=subscriber.source_scheduler,
        #                 )
        #             else:
        #                 flattened_flowable = FlatMergeNoBackpressureFlowable(
        #                     source=shared_flowable,
        #                     selector=selector,
        #                     subscribe_scheduler=subscriber.source_scheduler,
        #                 )
        #
        #             flowable = init_flowable(RefCountFlowable(flattened_flowable, stack=self.stack))
        #             yield key, flowable
        #
        #     return from_state(dict(gen_flowables()))

        # return subscription.copy(observable=LiftedMultiCastObservable(
        #     source=subscription.observable,
        #     func=func,
        #     scheduler=subscriber.multicast_scheduler,
        # ))
