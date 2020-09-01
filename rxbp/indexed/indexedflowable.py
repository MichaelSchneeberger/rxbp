import functools
import itertools
from abc import ABC, abstractmethod
from traceback import FrameSummary
from typing import Callable, Any, Tuple, Iterator, List
from typing import Generic

import rx

from rxbp.acknowledgement.ack import Ack
from rxbp.flowables.anonymousflowablebase import AnonymousFlowableBase
from rxbp.flowables.bufferflowable import BufferFlowable
from rxbp.flowables.defaultifemptyflowable import DefaultIfEmptyFlowable
from rxbp.flowables.doactionflowable import DoActionFlowable
from rxbp.flowables.firstflowable import FirstFlowable
from rxbp.flowables.firstordefaultflowable import FirstOrDefaultFlowable
from rxbp.flowables.flatmapflowable import FlatMapFlowable
from rxbp.flowables.init.initdebugflowable import init_debug_flowable
from rxbp.flowables.mapflowable import MapFlowable
from rxbp.flowables.maptoiteratorflowable import MapToIteratorFlowable
from rxbp.flowables.mergeflowable import MergeFlowable
from rxbp.flowables.observeonflowable import ObserveOnFlowable
from rxbp.flowables.pairwiseflowable import PairwiseFlowable
from rxbp.flowables.reduceflowable import ReduceFlowable
from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.flowables.repeatfirstflowable import RepeatFirstFlowable
from rxbp.flowables.scanflowable import ScanFlowable
from rxbp.flowables.tolistflowable import ToListFlowable
from rxbp.flowables.zipwithindexflowable import ZipWithIndexFlowable
from rxbp.indexed.flowables.concatindexedflowable import ConcatIndexedFlowable
from rxbp.indexed.flowables.controlledzipindexedflowable import ControlledZipIndexedFlowable
from rxbp.indexed.flowables.filterindexedflowable import FilterIndexedFlowable
from rxbp.indexed.flowables.matchindexedflowable import MatchIndexedFlowable
from rxbp.indexed.flowables.zipindexedflowable import ZipIndexedFlowable
from rxbp.indexed.indexedsubscription import IndexedSubscription
from rxbp.indexed.mixins.indexedflowablemixin import IndexedFlowableMixin
from rxbp.indexed.selectors.flowablebase import FlowableBase
from rxbp.mixins.flowableabsopmixin import FlowableAbsOpMixin
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observerinfo import ObserverInfo
from rxbp.pipeoperation import PipeOperation
from rxbp.scheduler import Scheduler
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.toiterator import to_iterator
from rxbp.torx import to_rx
from rxbp.typing import ValueType


class IndexedFlowable(FlowableAbsOpMixin, IndexedFlowableMixin, Generic[ValueType], ABC):
    @property
    @abstractmethod
    def underlying(self) -> IndexedFlowableMixin:
        ...

    def unsafe_subscribe(self, subscriber: Subscriber) -> IndexedSubscription:
        return self.underlying.unsafe_subscribe(subscriber=subscriber)

    @abstractmethod
    def _copy(self, flowable: IndexedFlowableMixin) -> 'IndexedFlowable':
        ...

    def buffer(self, buffer_size: int = None) -> IndexedFlowableMixin:
        flowable = BufferFlowable(source=self, buffer_size=buffer_size)
        return self._copy(flowable)

    def concat(self, *others: FlowableMixin) -> IndexedFlowableMixin:
        if len(others) == 0:
            return self

        all_sources = itertools.chain([self], others)
        return self._copy(ConcatIndexedFlowable(sources=list(all_sources)))

    def controlled_zip(
            self,
            right: FlowableMixin,
            request_left: Callable[[Any, Any], bool] = None,
            request_right: Callable[[Any, Any], bool] = None,
            match_func: Callable[[Any, Any], bool] = None,
    ) -> IndexedFlowableMixin:

        flowable = ControlledZipIndexedFlowable(
            left=self,
            right=right,
            request_left=request_left,
            request_right=request_right,
            match_func=match_func,
        )
        return self._copy(flowable)

    def debug(
            self,
            name: str,
            on_next: Callable[[Any], Ack],
            on_completed: Callable[[], None] = None,
            on_error: Callable[[Exception], None] = None,
            on_sync_ack: Callable[[Ack], None] = None,
            on_async_ack: Callable[[Ack], None] = None,
            on_observe: Callable[[ObserverInfo], None] = None,
            on_subscribe: Callable[[Subscriber], None] = None,
            on_raw_ack: Callable[[Ack], None] = None,
            stack: List[FrameSummary] = None,
            verbose: bool = None
    ):

        return self._copy(init_debug_flowable(
            source=self,
            name=name,
            on_next=on_next,
            on_completed=on_completed,
            on_error=on_error,
            on_observe=on_observe,
            on_subscribe=on_subscribe,
            on_sync_ack=on_sync_ack,
            on_async_ack=on_async_ack,
            on_raw_ack=on_raw_ack,
            stack=stack,
            verbose=verbose,
        ))

    def default_if_empty(self, lazy_val: Callable[[], Any]) -> IndexedFlowableMixin:
        return self._copy(DefaultIfEmptyFlowable(source=self, lazy_val=lazy_val))

    def do_action(
            self,
            on_next: Callable[[Any], None] = None,
            on_completed: Callable[[], None] = None,
            on_error: Callable[[Exception], None] = None,
            on_disposed: Callable[[], None] = None,
    ) -> IndexedFlowableMixin:
        return self._copy(DoActionFlowable(
            source=self,
            on_next=on_next,
            on_completed=on_completed,
            on_error=on_error,
            on_disposed=on_disposed,
        ))

    def execute_on(self, scheduler: Scheduler):
        return self._copy(ExecuteOnFlowable(source=self, scheduler=scheduler))

    def filter(
            self,
            predicate: Callable[[Any], bool],
            stack: List[FrameSummary],
    ) -> IndexedFlowableMixin:
        flowable = FilterIndexedFlowable(
            source=self,
            predicate=predicate,
            stack=stack,
        )
        return self._copy(flowable)

    def first(self, raise_exception: Callable[[Callable[[], None]], None]):
        flowable = FirstFlowable(source=self, raise_exception=raise_exception)
        return self._copy(flowable)

    def first_or_default(self, lazy_val: Callable[[], Any]):
        flowable = FirstOrDefaultFlowable(source=self, lazy_val=lazy_val)
        return self._copy(flowable)

    def flat_map(self, func: Callable[[Any], FlowableMixin]):
        flowable = FlatMapFlowable(source=self, func=func)
        return self._copy(flowable)

    def map(self, func: Callable[[ValueType], Any]) -> IndexedFlowableMixin:

        flowable = MapFlowable(source=self, func=func)
        return self._copy(flowable)

    def map_to_iterator(
            self,
            func: Callable[[ValueType], Iterator[ValueType]],
    ):
        flowable = MapToIteratorFlowable(source=self, func=func)
        return self._copy(flowable)

    def match(
            self,
            *others: IndexedFlowableMixin,
            left_debug: str,
            right_debug: str,
            stack: List[FrameSummary],
    ) -> 'IndexedFlowable':

        if len(others) == 0:
            return self.map(lambda v: (v,))
        else:
            sources = (self,) + others

            def gen_stack():
                for source in reversed(sources):
                    def _(right: 'IndexedFlowable' = None, left: 'IndexedFlowable' = source):
                        if right is None:
                            return left.map(lambda v: (v,))
                        else:
                            def inner_result_selector(t: Tuple[Any, Tuple[Any]]):
                                return (t[0],) + t[1]

                            flowable = MapFlowable(
                                source=MatchIndexedFlowable(
                                    left=left,
                                    right=right,
                                    left_debug=left_debug,
                                    right_debug=right_debug,
                                    stack=stack,
                                ),
                                func=inner_result_selector,
                            )
                            return flowable

                    yield _

            flowable: 'IndexedFlowable' = functools.reduce(lambda acc, v: v(acc), gen_stack(), None)
            return self._copy(flowable=flowable)

    def merge(self, *others: IndexedFlowableMixin):

        if len(others) == 0:
            return self
        else:
            sources = (self,) + others

            def gen_stack():
                for source in reversed(sources):
                    def _(right: IndexedFlowableMixin = None, left: IndexedFlowableMixin = source):
                        if right is None:
                            return left
                        else:
                            flowable = MergeFlowable(source=left, other=right)
                            return flowable

                    yield _

            flowable: 'IndexedFlowable' = functools.reduce(lambda acc, v: v(acc), gen_stack(), None)
            return self._copy(flowable=flowable)

    def observe_on(self, scheduler: Scheduler):

        return self._copy(ObserveOnFlowable(source=self, scheduler=scheduler))

    def pairwise(self) -> IndexedFlowableMixin:

        return self._copy(PairwiseFlowable(source=self))

    def pipe(self, *operators: PipeOperation[FlowableAbsOpMixin]) -> IndexedFlowableMixin:
        raw = functools.reduce(lambda obs, op: op(obs), operators, self)
        # return self._copy(raw)
        return raw

    def reduce(
            self,
            func: Callable[[Any, Any], Any],
            initial: Any,
    ):
        flowable = ReduceFlowable(
            source=self,
            func=func,
            initial=initial,
        )
        return self._copy(flowable)

    def repeat_first(self):

        flowable = RepeatFirstFlowable(source=self)
        return self._copy(flowable)

    def run(self, scheduler: Scheduler = None):
        return list(to_iterator(source=self, scheduler=scheduler))

    def scan(self, func: Callable[[Any, Any], Any], initial: Any):
        flowable = ScanFlowable(source=self, func=func, initial=initial)
        return self._copy(flowable)

    def share(self) -> IndexedFlowableMixin:
        return self._copy(super().share())

    def _share(self, stack: List[FrameSummary]):
        return self._copy(RefCountFlowable(source=self))

    def set_base(self, val: FlowableBase):

        def unsafe_unsafe_subscribe(subscriber: Subscriber) -> Subscription:
            subscription = self.unsafe_subscribe(subscriber=subscriber)

            return subscription.copy(
                base=val,
                observable=subscription.observable,
            )

        flowable = AnonymousFlowableBase(
            unsafe_subscribe_func=unsafe_unsafe_subscribe,
        )
        return self._copy(flowable)

    def to_list(self):

        flowable = ToListFlowable(source=self)
        return self._copy(flowable)

    def to_rx(self, batched: bool = None) -> rx.Observable:
        """ Converts this Flowable to an rx.Observable

        :param batched: if True, then the elements emitted by the Observable are expected to
        be a list or an iterator.
        """

        return to_rx(source=self, batched=batched)

    def zip(self, *others: IndexedFlowableMixin):

        if len(others) == 0:
            return self.map(lambda v: (v,))
        else:
            sources = (self,) + others

            def gen_stack():
                for source in reversed(sources):
                    def _(right: IndexedFlowableMixin = None, left: IndexedFlowableMixin = source):
                        if right is None:
                            return left.map(lambda v: (v,))
                        else:
                            def inner_result_selector(t: Tuple[Any, Tuple[Any]]):
                                return (t[0],) + t[1]

                            flowable = MapFlowable(
                                source=ZipIndexedFlowable(left=left, right=right),
                                func=inner_result_selector,
                            )
                            return flowable

                    yield _

            flowable = functools.reduce(lambda acc, v: v(acc), gen_stack(), None)
            return self._copy(flowable=flowable)

    def zip_with_index(self, selector: Callable[[Any, int], Any] = None):

        flowable = ZipWithIndexFlowable(source=self, selector=selector)
        return self._copy(flowable)