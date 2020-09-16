import functools
from abc import ABC
from typing import Generic

from rxbp.indexed.mixins.indexedflowableopmixin import IndexedFlowableOpMixin
from rxbp.mixins.flowableabsopmixin import FlowableAbsOpMixin
from rxbp.mixins.sharedflowablemixin import SharedFlowableMixin
from rxbp.pipeoperation import PipeOperation
from rxbp.scheduler import Scheduler
from rxbp.toiterator import to_iterator
from rxbp.typing import ValueType
from rxbp.utils.getstacklines import get_stack_lines


class IndexedFlowable(
    IndexedFlowableOpMixin,
    Generic[ValueType],
    ABC,
):
    def share(self) -> 'IndexedFlowable':
        stack = get_stack_lines()

        return self._share(stack=stack)

    def run(self, scheduler: Scheduler = None):
        return list(to_iterator(source=self, scheduler=scheduler))

    def pipe(self, *operators: PipeOperation[FlowableAbsOpMixin]) -> 'IndexedFlowable':
        flowable = functools.reduce(lambda obs, op: op(obs), operators, self)

        if isinstance(flowable, SharedFlowableMixin):
            return flowable.share()
        else:
            return flowable

    # @property
    # @abstractmethod
    # def underlying(self) -> IndexedFlowableMixin:
    #     ...
    #
    # def unsafe_subscribe(self, subscriber: Subscriber) -> IndexedSubscription:
    #     return self.underlying.unsafe_subscribe(subscriber=subscriber)
    #
    # @abstractmethod
    # def _copy(self, **kwargs) -> 'IndexedFlowable':
    #     ...
    #
    # def buffer(self, buffer_size: int = None) -> IndexedFlowableMixin:
    #     flowable = BufferFlowable(source=self, buffer_size=buffer_size)
    #     return self._copy(flowable)
    #
    # def concat(self, *others: FlowableMixin) -> IndexedFlowableMixin:
    #     if len(others) == 0:
    #         return self
    #
    #     all_sources = itertools.chain([self], others)
    #     return self._copy(underlying=ConcatIndexedFlowable(sources=list(all_sources)))
    #
    # def controlled_zip(
    #         self,
    #         right: FlowableMixin,
    #         request_left: Callable[[Any, Any], bool] = None,
    #         request_right: Callable[[Any, Any], bool] = None,
    #         match_func: Callable[[Any, Any], bool] = None,
    # ) -> IndexedFlowableMixin:
    #
    #     flowable = ControlledZipIndexedFlowable(
    #         left=self,
    #         right=right,
    #         request_left=request_left,
    #         request_right=request_right,
    #         match_func=match_func,
    #     )
    #     return self._copy(underlying=flowable)
    #
    # def debug(
    #         self,
    #         name: str,
    #         on_next: Callable[[Any], Ack],
    #         on_completed: Callable[[], None] = None,
    #         on_error: Callable[[Exception], None] = None,
    #         on_sync_ack: Callable[[Ack], None] = None,
    #         on_async_ack: Callable[[Ack], None] = None,
    #         on_observe: Callable[[ObserverInfo], None] = None,
    #         on_subscribe: Callable[[Subscriber], None] = None,
    #         on_raw_ack: Callable[[Ack], None] = None,
    #         stack: List[FrameSummary] = None,
    #         verbose: bool = None
    # ):
    #
    #     return self._copy(underlying=init_debug_flowable(
    #         source=self,
    #         name=name,
    #         on_next=on_next,
    #         on_completed=on_completed,
    #         on_error=on_error,
    #         on_observe=on_observe,
    #         on_subscribe=on_subscribe,
    #         on_sync_ack=on_sync_ack,
    #         on_async_ack=on_async_ack,
    #         on_raw_ack=on_raw_ack,
    #         stack=stack,
    #         verbose=verbose,
    #     ))
    #
    # def debug_base(
    #         self,
    #         base: FlowableBase,
    #         stack: List[FrameSummary],
    #         name: str = None,
    # ):
    #     if name is None:
    #         name = str(base)
    #
    #     return self._copy(underlying=DebugBaseIndexedFlowable(
    #         source=self,
    #         base=base,
    #         name=name,
    #         stack=stack,
    #     ))
    #
    # def default_if_empty(self, lazy_val: Callable[[], Any]) -> IndexedFlowableMixin:
    #     return self._copy(underlying=DefaultIfEmptyFlowable(source=self, lazy_val=lazy_val))
    #
    # def do_action(
    #         self,
    #         on_next: Callable[[Any], None] = None,
    #         on_completed: Callable[[], None] = None,
    #         on_error: Callable[[Exception], None] = None,
    #         on_disposed: Callable[[], None] = None,
    # ) -> IndexedFlowableMixin:
    #     return self._copy(underlying=DoActionFlowable(
    #         source=self,
    #         on_next=on_next,
    #         on_completed=on_completed,
    #         on_error=on_error,
    #         on_disposed=on_disposed,
    #     ))
    #
    # def filter(
    #         self,
    #         predicate: Callable[[Any], bool],
    #         stack: List[FrameSummary],
    # ) -> IndexedFlowableMixin:
    #     flowable = FilterIndexedFlowable(
    #         source=self,
    #         predicate=predicate,
    #         stack=stack,
    #     )
    #     return self._copy(underlying=flowable)
    #
    # def first(self, raise_exception: Callable[[Callable[[], None]], None]):
    #     flowable = FirstFlowable(source=self, raise_exception=raise_exception)
    #     return self._copy(underlying=flowable)
    #
    # def first_or_default(self, lazy_val: Callable[[], Any]):
    #     flowable = FirstOrDefaultFlowable(source=self, lazy_val=lazy_val)
    #     return self._copy(underlying=flowable)
    #
    # def flat_map(self, func: Callable[[Any], FlowableMixin]):
    #     flowable = FlatMapFlowable(source=self, func=func)
    #     return self._copy(underlying=flowable)
    #
    # def map(self, func: Callable[[ValueType], Any]) -> IndexedFlowableMixin:
    #
    #     flowable = MapFlowable(source=self, func=func)
    #     return self._copy(underlying=flowable)
    #
    # def map_to_iterator(
    #         self,
    #         func: Callable[[ValueType], Iterator[ValueType]],
    # ):
    #     flowable = MapToIteratorFlowable(source=self, func=func)
    #     return self._copy(underlying=flowable)
    #
    # def match(
    #         self,
    #         *others: IndexedFlowableMixin,
    #         stack: List[FrameSummary],
    #         left_debug: Optional[str] = None,
    #         right_debug: Optional[str] = None,
    # ) -> 'IndexedFlowable':
    #
    #     if len(others) == 0:
    #         return self.map(lambda v: (v,))
    #     else:
    #         sources = (self,) + others
    #
    #         def gen_stack():
    #             for source in reversed(sources):
    #                 def _(right: 'IndexedFlowable' = None, left: 'IndexedFlowable' = source):
    #                     if right is None:
    #                         return left.map(lambda v: (v,))
    #                     else:
    #                         def inner_result_selector(t: Tuple[Any, Tuple[Any]]):
    #                             return (t[0],) + t[1]
    #
    #                         flowable = MapFlowable(
    #                             source=MatchIndexedFlowable(
    #                                 left=left,
    #                                 right=right,
    #                                 left_debug=left_debug,
    #                                 right_debug=right_debug,
    #                                 stack=stack,
    #                             ),
    #                             func=inner_result_selector,
    #                         )
    #                         return flowable
    #
    #                 yield _
    #
    #         flowable: 'IndexedFlowable' = functools.reduce(lambda acc, v: v(acc), gen_stack(), None)
    #            return self._copy(underlying=flowable)
    #
    # def merge(self, *others: IndexedFlowableMixin):
    #
    #     if len(others) == 0:
    #         return self
    #     else:
    #         sources = (self,) + others
    #
    #         def gen_stack():
    #             for source in reversed(sources):
    #                 def _(right: IndexedFlowableMixin = None, left: IndexedFlowableMixin = source):
    #                     if right is None:
    #                         return left
    #                     else:
    #                         flowable = MergeFlowable(source=left, other=right)
    #                         return flowable
    #
    #                 yield _
    #
    #         flowable: 'IndexedFlowable' = functools.reduce(lambda acc, v: v(acc), gen_stack(), None)
    #         return self._copy(flowable=flowable)
    #
    # def observe_on(self, scheduler: Scheduler):
    #
    #     return self._copy(underlying=ObserveOnFlowable(source=self, scheduler=scheduler))
    #
    # def pairwise(self) -> IndexedFlowableMixin:
    #
    #     return self._copy(PairwiseFlowable(source=self))
    #
    # def reduce(
    #         self,
    #         func: Callable[[Any, Any], Any],
    #         initial: Any,
    # ):
    #     flowable = ReduceFlowable(
    #         source=self,
    #         func=func,
    #         initial=initial,
    #     )
    #     return self._copy(underlying=flowable)
    #
    # def repeat_first(self):
    #
    #     flowable = RepeatFirstFlowable(source=self)
    #     return self._copy(underlying=flowable)
    #
    # def run(self, scheduler: Scheduler = None):
    #     return list(to_iterator(source=self, scheduler=scheduler))
    #
    # def scan(self, func: Callable[[Any, Any], Any], initial: Any):
    #     flowable = ScanFlowable(source=self, func=func, initial=initial)
    #     return self._copy(underlying=flowable)
    #
    # def _share(self, stack: List[FrameSummary]):
    #     return self._copy(
    #         underlying=RefCountFlowable(source=self, stack=stack),
    #         is_shared=True,
    #     )
    #
    # def set_base(self, val: FlowableBase):
    #
    #     @dataclass
    #     class SetBaseIndexedFlowable(IndexedFlowableMixin):
    #         source: IndexedFlowableMixin
    #         base: FlowableBase
    #
    #         def unsafe_subscribe(self, subscriber: Subscriber) -> IndexedSubscription:
    #             subscription = self.source.unsafe_subscribe(subscriber=subscriber)
    #
    #             return subscription.copy(
    #                 index=FlowableBaseAndSelectors(base=self.base, selectors=None),
    #                 observable=subscription.observable,
    #             )
    #
    #     return self._copy(underlying=SetBaseIndexedFlowable(
    #         source=self,
    #         base=val,
    #     ))
    #
    # def to_list(self):
    #
    #     flowable = ToListFlowable(source=self)
    #     return self._copy(underlying=flowable)
    #
    # def to_rx(self, batched: bool = None) -> rx.Observable:
    #     """ Converts this Flowable to an rx.Observable
    #
    #     :param batched: if True, then the elements emitted by the Observable are expected to
    #     be a list or an iterator.
    #     """
    #
    #     return to_rx(source=self, batched=batched)
    #
    # def zip(self, *others: IndexedFlowableMixin):
    #
    #     if len(others) == 0:
    #         return self.map(lambda v: (v,))
    #     else:
    #         sources = (self,) + others
    #
    #         def gen_stack():
    #             for source in reversed(sources):
    #                 def _(right: IndexedFlowableMixin = None, left: IndexedFlowableMixin = source):
    #                     if right is None:
    #                         return left.map(lambda v: (v,))
    #                     else:
    #                         def inner_result_selector(t: Tuple[Any, Tuple[Any]]):
    #                             return (t[0],) + t[1]
    #
    #                         flowable = MapFlowable(
    #                             source=ZipIndexedFlowable(left=left, right=right),
    #                             func=inner_result_selector,
    #                         )
    #                         return flowable
    #
    #                 yield _
    #
    #         flowable = functools.reduce(lambda acc, v: v(acc), gen_stack(), None)
    #         return self._copy(flowable=flowable)
    #
    # def zip_with_index(self, selector: Callable[[Any, int], Any] = None):
    #
    #     flowable = ZipWithIndexFlowable(source=self, selector=selector)
    #     return self._copy(underlying=flowable)