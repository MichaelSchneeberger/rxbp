import functools
import itertools
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Any, Tuple, Iterator, Generic

import rx

from rxbp.ack.mixins.ackmixin import AckMixin
from rxbp.flowables.bufferflowable import BufferFlowable
from rxbp.flowables.concatflowable import ConcatFlowable
from rxbp.flowables.controlledzipflowable import ControlledZipFlowable
from rxbp.flowables.debugflowable import DebugFlowable
from rxbp.flowables.defaultifemptyflowable import DefaultIfEmptyFlowable
from rxbp.flowables.doactionflowable import DoActionFlowable
from rxbp.flowables.executeonflowable import ExecuteOnFlowable
from rxbp.flowables.fastfilterflowable import FastFilterFlowable
from rxbp.flowables.filterflowable import FilterFlowable
from rxbp.flowables.firstflowable import FirstFlowable
from rxbp.flowables.firstordefaultflowable import FirstOrDefaultFlowable
from rxbp.flowables.flatmapflowable import FlatMapFlowable
from rxbp.flowables.lastflowable import LastFlowable
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
from rxbp.flowables.zipflowable import ZipFlowable
from rxbp.flowables.zipwithindexflowable import ZipWithIndexFlowable
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.mixins.flowableoptemplatemixin import FlowableOpTemplateMixin
from rxbp.observerinfo import ObserverInfo
from rxbp.pipeoperation import PipeOperation
from rxbp.scheduler import Scheduler
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.toiterator import to_iterator
from rxbp.torx import to_rx
from rxbp.typing import ValueType


@dataclass
class Flowable(FlowableOpTemplateMixin, FlowableMixin, Generic[ValueType], ABC):
    @property
    @abstractmethod
    def underlying(self) -> FlowableMixin:
        ...

    # @property
    # @abstractmethod
    # def _sharable(self) -> bool:
    #     ...

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        return self.underlying.unsafe_subscribe(subscriber=subscriber)

    @abstractmethod
    def _copy(self, flowable: FlowableMixin, sharable: bool = None) -> 'Flowable':
        ...

    def pipe(self, *operators: PipeOperation['Flowable']) -> 'Flowable':
        raw = functools.reduce(lambda obs, op: op(obs), operators, self)
        return self._copy(raw)

    def buffer(self, buffer_size: int = None) -> 'Flowable':
        flowable = BufferFlowable(source=self, buffer_size=buffer_size)
        return self._copy(flowable)

    def concat(self, *others: FlowableMixin) -> 'Flowable':
        if len(others) == 0:
            return self

        all_sources = itertools.chain([self], others)
        flowable = ConcatFlowable(sources=list(all_sources))
        return self._copy(flowable)

    def controlled_zip(
            self,
            right: FlowableMixin,
            request_left: Callable[[Any, Any], bool] = None,
            request_right: Callable[[Any, Any], bool] = None,
            match_func: Callable[[Any, Any], bool] = None,
    ) -> 'Flowable':

        assert isinstance(right, FlowableMixin), f'"{right}" must be of type FlowableMixin'

        flowable = ControlledZipFlowable(
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
            on_next: Callable[[Any], AckMixin] = None,
            on_completed: Callable[[], None] = None,
            on_error: Callable[[Exception], None] = None,
            on_ack: Callable[[AckMixin], None] = None,
            on_subscribe: Callable[[ObserverInfo], None] = None,
            on_raw_ack: Callable[[AckMixin], None] = None,
    ) -> 'Flowable':

        return self._copy(DebugFlowable(
            source=self,
            name=name,
            on_next=on_next,
            on_completed=on_completed,
            on_error=on_error,
            on_subscribe=on_subscribe,
            on_ack=on_ack,
            on_raw_ack=on_raw_ack,
        ))

    def default_if_empty(self, lazy_val: Callable[[], Any]) -> 'Flowable':
        return self._copy(DefaultIfEmptyFlowable(source=self, lazy_val=lazy_val))

    def do_action(
            self,
            on_next: Callable[[Any], None] = None,
            on_completed: Callable[[], None] = None,
            on_error: Callable[[Exception], None] = None,
            on_disposed: Callable[[], None] = None,
    ) -> 'Flowable':
        return self._copy(DoActionFlowable(
            source=self,
            on_next=on_next,
            on_completed=on_completed,
            on_error=on_error,
            on_disposed=on_disposed,
        ))

    def execute_on(self, scheduler: Scheduler):
        return self._copy(ExecuteOnFlowable(source=self, scheduler=scheduler))

    def fast_filter(self, predicate: Callable[[Any], bool]) -> 'Flowable':
        flowable = FastFilterFlowable(source=self, predicate=predicate)
        return self._copy(flowable)

    def filter(self, predicate: Callable[[Any], bool]) -> 'Flowable':

        flowable = FilterFlowable(source=self, predicate=predicate)
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

    def last(self, raise_exception: Callable[[Callable[[], None]], None]):
        flowable = LastFlowable(source=self, raise_exception=raise_exception)
        return self._copy(flowable)

    def map(self, func: Callable[[ValueType], Any]):

        flowable = MapFlowable(source=self, func=func)
        return self._copy(flowable)

    def map_to_iterator(
            self,
            func: Callable[[ValueType], Iterator[ValueType]],
    ):
        flowable = MapToIteratorFlowable(source=self, func=func)
        return self._copy(flowable)

    def merge(self, *others: FlowableMixin):

        assert all(isinstance(source, FlowableMixin) for source in others), \
            f'"{others}" must all be of type FlowableMixin'

        if len(others) == 0:
            return self
        else:
            sources = (self,) + others

            def gen_stack():
                for source in reversed(sources):
                    def _(right: Flowable = None, left: Flowable = source):
                        if right is None:
                            return left
                        else:
                            flowable = MergeFlowable(source=left, other=right)
                            return flowable

                    yield _

            obs: FlowableMixin = functools.reduce(lambda acc, v: v(acc), gen_stack(), None)
            return self._copy(obs)

    def observe_on(self, scheduler: Scheduler):

        return self._copy(ObserveOnFlowable(source=self, scheduler=scheduler))

    def pairwise(self) -> 'Flowable':

        return self._copy(PairwiseFlowable(source=self))

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

    # def share(self) -> 'Flowable':
    #     return self._copy(super().share())

    def share(self):
        if self._sharable:
            return self._copy(RefCountFlowable(source=self))
        else:
            raise Exception('this Flowable cannot be shared, use ??? instead')

    # def set_base(self, val: Base):
    #
    #     def unsafe_unsafe_subscribe(subscriber: Subscriber) -> Subscription:
    #         subscription = self.unsafe_subscribe(subscriber=subscriber)
    #
    #         return subscription.copy(
    #             base=val,
    #             observable=subscription.observable,
    #         )
    #
    #     flowable = AnonymousFlowableBase(
    #         unsafe_subscribe_func=unsafe_unsafe_subscribe,
    #     )
    #     return self._copy(flowable)

    def to_list(self):

        flowable = ToListFlowable(source=self)
        return self._copy(flowable)

    def to_rx(self, batched: bool = None) -> rx.Observable:
        """ Converts this Flowable to an rx.Observable

        :param batched: if True, then the elements emitted by the Observable are expected to
        be a list or an iterator.
        """

        return to_rx(source=self, batched=batched)

    def zip(self, *others: 'Flowable'):

        assert all(isinstance(source, FlowableMixin) for source in others), \
            f'"{others}" must all be of type FlowableMixin'

        if len(others) == 0:
            return self.map(lambda v: (v,))
        else:
            sources = (self,) + others

            def gen_stack():
                for source in reversed(sources):
                    def _(right: 'Flowable' = None, left: 'Flowable' = source):
                        if right is None:
                            return left.map(lambda v: (v,))
                        else:
                            def inner_result_selector(v1: Any, v2: Tuple[Any]):
                                return (v1,) + v2

                            flowable = ZipFlowable(left=left, right=right, func=inner_result_selector)
                            return flowable

                    yield _

            obs: FlowableMixin = functools.reduce(lambda acc, v: v(acc), gen_stack(), None)
            return self._copy(obs)

    def zip_with_index(self, selector: Callable[[Any, int], Any] = None):

        flowable = ZipWithIndexFlowable(source=self, selector=selector)
        return self._copy(flowable)
