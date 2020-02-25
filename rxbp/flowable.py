import functools
import itertools
from typing import Callable, Any, Generic, Tuple, Iterator

import rx

from rxbp.flowablebase import FlowableBase
from rxbp.flowableopmixin import FlowableOpMixin
from rxbp.flowables.anonymousflowablebase import AnonymousFlowableBase
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
from rxbp.flowables.mapflowable import MapFlowable
from rxbp.flowables.maptoiteratorflowable import MapToIteratorFlowable
from rxbp.flowables.matchflowable import MatchFlowable
from rxbp.flowables.mergeflowable import MergeFlowable
from rxbp.flowables.observeonflowable import ObserveOnFlowable
from rxbp.flowables.pairwiseflowable import PairwiseFlowable
from rxbp.flowables.reduceflowable import ReduceFlowable
from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.flowables.repeatfirstflowable import RepeatFirstFlowable
from rxbp.flowables.scanflowable import ScanFlowable
from rxbp.flowables.tolistflowable import ToListFlowable
from rxbp.flowables.zip2flowable import Zip2Flowable
from rxbp.flowables.zipwithindexflowable import ZipWithIndexFlowable
from rxbp.scheduler import Scheduler
from rxbp.selectors.base import Base
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.toiterator import to_iterator
from rxbp.torx import to_rx
from rxbp.typing import ValueType


class Flowable(FlowableOpMixin, FlowableBase, Generic[ValueType]):
    """ A `Flowable` implements a `subscribe` method allowing to describe a
    data flow from source to sink. The "description" is
    done with *rxbp* operators exposed by `rxbp.op`.

    Like in functional programming, usings *rxbackpressure* operators does not create
    any mutable states but rather concatenates functions without calling them
    yet. Or in other words, we first describe what we want to do, and then
    we execute the plan. A `Flowable` is executed by calling its `subscribe`
    method. This will start a chain reaction, where downsream `Flowables`
    call the `subscribe` method of their linked upstream `Flowable` until
    the sources start emitting data. Once a `Flowable` is subscribed, we
    allow it to have mutable states where it make sense.

    Compared to RxPY Observables, a `Flowable` uses `Observers` that are
    able to back-pressure an `on_next` method call.
    """

    def __init__(self, underlying: FlowableBase):
        super().__init__()

        self.underlying = underlying

    @classmethod
    def _copy(cls, flowable: FlowableBase):
        return cls(flowable)

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        return self.underlying.unsafe_subscribe(subscriber=subscriber)

    def buffer(self, buffer_size: int = None) -> 'Flowable':
        flowable = BufferFlowable(source=self, buffer_size=buffer_size)
        return self._copy(flowable)

    def concat(self, *others: FlowableBase) -> 'Flowable':
        if len(others) == 0:
            return self

        all_sources = itertools.chain([self], others)
        flowable = ConcatFlowable(sources=list(all_sources))
        return self._copy(flowable)

    def controlled_zip(
            self,
            right: FlowableBase,
            request_left: Callable[[Any, Any], bool] = None,
            request_right: Callable[[Any, Any], bool] = None,
            match_func: Callable[[Any, Any], bool] = None,
    ) -> 'Flowable[ValueType]':

        flowable = ControlledZipFlowable(
            left=self,
            right=right,
            request_left=request_left,
            request_right=request_right,
            match_func=match_func,
        )
        return self._copy(flowable)

    def debug(self, name=None, on_next=None, on_subscribe=None, on_ack=None, on_raw_ack=None, on_ack_msg=None):

        return self._copy(DebugFlowable(
            source=self,
            name=name,
            on_next=on_next,
            on_subscribe=on_subscribe,
            on_ack=on_ack,
            on_raw_ack=on_raw_ack,
            on_ack_msg=on_ack_msg,
        ))

    def default_if_empty(self, lazy_val: Callable[[], Any]):
        return self._copy(DefaultIfEmptyFlowable(source=self, lazy_val=lazy_val))

    def do_action(
            self,
            on_next: Callable[[Any], None] = None,
            on_completed: Callable[[], None] = None,
            on_error: Callable[[Exception], None] = None,
            on_disposed: Callable[[], None] = None,
    ):
        return self._copy(DoActionFlowable(
            source=self,
            on_next=on_next,
            on_completed=on_completed,
            on_error=on_error,
            on_disposed=on_disposed,
        ))

    def execute_on(self, scheduler: Scheduler):
        return self._copy(ExecuteOnFlowable(source=self, scheduler=scheduler))

    def fast_filter(self, predicate: Callable[[Any], bool]) -> 'Flowable[ValueType]':
        flowable = FastFilterFlowable(source=self, predicate=predicate)
        return self._copy(flowable)

    def filter(self, predicate: Callable[[Any], bool]) -> 'Flowable[ValueType]':

        flowable = FilterFlowable(source=self, predicate=predicate)
        return self._copy(flowable)

    def first(self, raise_exception: Callable[[Callable[[], None]], None]):
        flowable = FirstFlowable(source=self, raise_exception=raise_exception)
        return self._copy(flowable)

    def first_or_default(self, lazy_val: Callable[[], Any]):
        flowable = FirstOrDefaultFlowable(source=self, lazy_val=lazy_val)
        return self._copy(flowable)

    def flat_map(self, func: Callable[[Any], FlowableBase]):
        flowable = FlatMapFlowable(source=self, func=func)
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

    def match(self, *others: 'Flowable'):

        if len(others) == 0:
            return self.map(lambda v: (v,))
        else:
            sources = (self,) + others

            def gen_stack():
                for source in reversed(sources):
                    def _(right: Flowable = None, left: Flowable = source):
                        if right is None:
                            return left.map(lambda v: (v,))
                        else:
                            def inner_result_selector(v1: Any, v2: Tuple[Any]):
                                return (v1,) + v2

                            flowable = MatchFlowable(left=left, right=right, func=inner_result_selector)
                            return flowable

                    yield _

            obs = functools.reduce(lambda acc, v: v(acc), gen_stack(), None)
            return self._copy(obs)

    def merge(self, *others: 'Flowable'):

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

            obs = functools.reduce(lambda acc, v: v(acc), gen_stack(), None)
            return self._copy(obs)

    def observe_on(self, scheduler: Scheduler):

        return self._copy(ObserveOnFlowable(source=self, scheduler=scheduler))

    def pairwise(self) -> 'Flowable':

        return self._copy(PairwiseFlowable(source=self))

    def pipe(self, *operators: Callable[[FlowableOpMixin], FlowableOpMixin]) -> 'Flowable':
        raw = functools.reduce(lambda obs, op: op(obs), operators, self)
        return self._copy(raw)

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

    def share(self) -> 'Flowable':
        return self._copy(super().share())

    def _share(self):
        return self._copy(RefCountFlowable(source=self))

    def set_base(self, val: Base):

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

    def zip(self, *others: 'Flowable'):

        if len(others) == 0:
            return self.map(lambda v: (v,))
        else:
            sources = (self,) + others

            def gen_stack():
                for source in reversed(sources):
                    def _(right: Flowable = None, left: Flowable = source):
                        if right is None:
                            return left.map(lambda v: (v,))
                        else:
                            def inner_result_selector(v1: Any, v2: Tuple[Any]):
                                return (v1,) + v2

                            flowable = Zip2Flowable(left=left, right=right, func=inner_result_selector)
                            return flowable

                    yield _

            obs = functools.reduce(lambda acc, v: v(acc), gen_stack(), None)
            return self._copy(obs)

    def zip_with_index(self, selector: Callable[[Any, int], Any] = None):

        flowable = ZipWithIndexFlowable(source=self, selector=selector)
        return self._copy(flowable)
