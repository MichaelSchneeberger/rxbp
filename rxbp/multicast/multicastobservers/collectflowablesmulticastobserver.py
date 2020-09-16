import types
from dataclasses import dataclass
from itertools import chain
from traceback import FrameSummary
from typing import List

from rx.disposable import RefCountDisposable

from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.init.initsharedflowable import init_shared_flowable
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.multicast.flowables.connectableflowable import ConnectableFlowable
from rxbp.multicast.flowables.flatconcatnobackpressureflowable import \
    FlatConcatNoBackpressureFlowable
from rxbp.multicast.flowables.flatmergenobackpressureflowable import \
    FlatMergeNoBackpressureFlowable
from rxbp.multicast.mixins.flowablestatemixin import FlowableStateMixin
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.typing import MultiCastItem
from rxbp.observer import Observer
from rxbp.typing import ElementType


@dataclass
class CollectFlowablesMultiCastObserver(MultiCastObserver):
    next_observer: MultiCastObserver
    ref_count_disposable: RefCountDisposable
    maintain_order: bool
    stack: List[FrameSummary]
    subscriber: MultiCastSubscriber

    def __post_init__(self):
        self.is_first = True
        self.inner_observer: Observer = None

    def on_next(self, item: MultiCastItem) -> None:
        if isinstance(item, list):
            if len(item) == 0:
                return

            first_elem = item[0]
            flowable_states = item

        else:
            try:
                first_elem = next(item)
                flowable_states = chain([first_elem], item)
            except StopIteration:
                return

        if isinstance(first_elem, dict):
            to_state = lambda s: s
            from_state = lambda s: s

        elif isinstance(first_elem, FlowableStateMixin):
            to_state = lambda s: s.get_flowable_state()

            def from_state(state):
                value = first_elem.set_flowable_state(state)
                assert isinstance(value, FlowableStateMixin), f'"{value}" should be of type FlowableStateMixin'
                return value

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

        # UpstreamObserver --> ConnectableObserver --> RefCountObserver --> FlatNoBackpressureObserver

        @dataclass
        class ConnectableObserver(Observer):
            underlying: Observer
            outer_observer: MultiCastObserver

            def connect(self):
                pass

            def on_next(self, elem: ElementType):
                return self.underlying.on_next(elem)

            def on_error(self, err):
                self.outer_observer.on_error(err)
                self.underlying.on_error(err)

            def on_completed(self):
                self.underlying.on_completed()

        conn_observer = ConnectableObserver(
            underlying=None,
            outer_observer=self.next_observer,
        )

        # buffered_observer = BufferedObserver(
        #     underlying=conn_observer,
        #     scheduler=subscriber.multicast_scheduler,
        #     subscribe_scheduler=subscriber.multicast_scheduler,
        #     buffer_size=1000,
        # )

        self.inner_observer = conn_observer

        inner_disposable = self.ref_count_disposable.disposable
        conn_flowable = ConnectableFlowable(
            conn_observer=conn_observer,
            disposable=inner_disposable,
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
                        subscribe_scheduler=self.subscriber.subscribe_schedulers[0],
                    )
                else:
                    flattened_flowable = FlatMergeNoBackpressureFlowable(
                        source=shared_flowable,
                        selector=selector,
                        subscribe_scheduler=self.subscriber.subscribe_schedulers[0],
                    )

                flowable = init_shared_flowable(RefCountFlowable(flattened_flowable, stack=self.stack))
                yield key, flowable

        flowable_imitations = from_state(dict(gen_flowables()))
        self.next_observer.on_next([flowable_imitations])
        self.next_observer.on_completed()

        # observable didn't get subscribed
        if conn_observer.underlying is None:
            def on_next_if_not_subscribed(self, val):
                pass

            self.on_next = types.MethodType(on_next_if_not_subscribed, self)

            # if there is no inner subscriber, dispose the source
            inner_disposable.dispose()
            return

        self.is_first = False

        _ = self.inner_observer.on_next(flowable_states)

        def on_next_after_first(self, elem: MultiCastItem):
            _ = self.inner_observer.on_next(elem)

        self.on_next = types.MethodType(on_next_after_first, self)  # type: ignore

    def on_error(self, exc: Exception) -> None:
        if self.is_first:
            self.next_observer.on_error(exc)
        else:
            self.inner_observer.on_error(exc)

    def on_completed(self) -> None:
        if self.is_first:
            self.next_observer.on_completed()
        else:
            self.inner_observer.on_completed()
