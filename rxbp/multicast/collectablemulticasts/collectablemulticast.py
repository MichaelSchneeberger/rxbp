from functools import reduce
from typing import Callable, Union, List, Dict

from rxbp.flowable import Flowable
from rxbp.multicast.flowablestatemixin import FlowableStateMixin
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicastoperator import MultiCastOperator
from rxbp.multicast.multicastopmixin import MultiCastOpMixin
from rxbp.multicast.typing import MultiCastValue
from rxbp.typing import ValueType


class CollectableMultiCast(MultiCastOpMixin):
    def __init__(
            self,
            main: MultiCastOpMixin,
            collected: MultiCastOpMixin,
    ):
        self._main = main
        self._collected = collected

    @property
    def main_source(self):
        return self._main

    @property
    def collected_source(self):
        return self._collected

    def collect(self):
        collected = self._main.merge(self._collected)
        return CollectableMultiCast(main=self._main.empty(), collected=collected)

    def merge_collected(self):
        return self._main.merge(self._collected)

    def debug(self, name: str = None) -> 'CollectableMultiCast':
        main = self._main.debug(name=name)
        return CollectableMultiCast(main=main, collected=self._collected)

    def loop_flowable(self, func: Callable[[MultiCastValue], MultiCastValue], initial: ValueType):
        raise NotImplementedError

    def empty(self):
        return CollectableMultiCast(main=self._main.empty(), collected=self._collected.empty())

    def share_flowable(self, func: Callable[[MultiCastValue], Union[Flowable, List, Dict, FlowableStateMixin]]):
        main = self._main.share_flowable(func=func)
        return CollectableMultiCast(main=main, collected=self._collected)

    def filter(self, predicate: Callable[[MultiCastValue], bool]):
        main = self._main.filter(predicate=predicate)
        return CollectableMultiCast(main=main, collected=self._collected)

    def flat_map(self, func: Callable[[MultiCastValue], 'MultiCastOpMixin']):
        shared_source = self._main.map(func=func).share()
        main = shared_source.flat_map(lambda c: c.main_source)
        collected = shared_source.flat_map(lambda c: c.collected_source).merge(self._collected)
        return CollectableMultiCast(main=main, collected=collected)

    def lift(self, func: Callable[[MultiCast], MultiCastValue]):
        main = self._main.lift(func=func)
        return CollectableMultiCast(main=main, collected=self._collected)

    def merge(self, *others: 'CollectableMultiCast'):
        main = self._main.merge(*(source.main_source for source in others))
        collected = self._collected.merge(*(source.collected_source for source in others))
        return CollectableMultiCast(main=main, collected=collected)

    def map(self, func: Callable[[MultiCastValue], MultiCastValue]):
        main = self._main.map(func=func)
        return CollectableMultiCast(main=main, collected=self._collected)

    def pipe(self, *operators: MultiCastOperator) -> 'CollectableMultiCast':
        return reduce(lambda acc, op: op(acc), operators, self)

    def reduce_flowable(
            self,
            maintain_order: bool = None,
    ):
        main = self._main.reduce_flowable(maintain_order=maintain_order)
        return CollectableMultiCast(main=main, collected=self._collected)

    def share(self):
        main = self._main.share()
        collected = self._collected.share()
        return CollectableMultiCast(main=main, collected=collected)

    def collect_flowables(self, *others: 'CollectableMultiCast'):
        main = self._main.collect_flowables(*(source.main_source for source in others))
        collected = self._collected.merge(*(source.collected_source for source in others))
        return CollectableMultiCast(main=main, collected=collected)

    # def buffer(self, buffer_size: int) -> 'PairedFlowable':
    #     first = self._first.buffer(buffer_size=buffer_size)
    #     return PairedFlowable(first=first, second=self._second)
    #
    # def concat(self, *sources: 'PairedFlowable') -> 'PairedFlowable':
    #     first = self._first.concat(*(source.main_source for source in sources))
    #     second = self._first.merge(*(source.collected_source for source in sources))
    #     return PairedFlowable(first=first, second=second)
    #
    # def controlled_zip(
    #         self,
    #         right: 'PairedFlowable',
    #         request_left: Callable[[Any, Any], bool] = None,
    #         request_right: Callable[[Any, Any], bool] = None,
    #         match_func: Callable[[Any, Any], bool] = None,
    # ) -> 'PairedFlowable':
    #     first = self._first.controlled_zip(
    #         right=right.main_source,
    #         request_left=request_left,
    #         request_right=request_right,
    #         match_func=match_func,
    #     )
    #     second = self._second.merge(right.collected_source)
    #     return PairedFlowable(first=first, second=second)
    #
    # def debug(self, name=None, on_next=None, on_subscribe=None, on_ack=None, on_raw_ack=None,
    #           on_ack_msg=None) -> 'PairedFlowable':
    #     first = self._first.debug(name=name, on_next=on_next, on_subscribe=on_subscribe, on_ack=on_ack,
    #                               on_raw_ack=on_raw_ack, on_ack_msg=on_ack_msg)
    #     return PairedFlowable(first=first, second=self._second)
    #
    # def fast_filter(self, predicate: Callable[[Any], bool]) -> 'PairedFlowable':
    #     first = self._first.fast_filter(predicate=predicate)
    #     return PairedFlowable(first=first, second=self._second)
    #
    # def filter(self, predicate: Callable[[Any], bool]) -> 'PairedFlowable':
    #     first = self._first.filter(predicate=predicate)
    #     return PairedFlowable(first=first, second=self._second)
    #
    # def filter_with_index(self, predicate: Callable[[Any, int], bool]) -> 'PairedFlowable':
    #     first = self._first.filter_with_index(predicate=predicate)
    #     return PairedFlowable(first=first, second=self._second)
    #
    # def first(self, raise_exception: Callable[[Callable[[], None]], None] = None) -> 'PairedFlowable':
    #     first = self._first.first(raise_exception=raise_exception)
    #     return PairedFlowable(first=first, second=self._second)
    #
    # def flat_map(self, selector: Callable[[Any], 'PairedFlowable']) -> 'PairedFlowable':
    #     first = self._first.flat_map(selector=selector)
    #     return PairedFlowable(first=first, second=self._second)
    #
    # def map(self, selector: Callable[[Any], Any]) -> 'PairedFlowable':
    #     first = self._first.map(selector=selector)
    #     return PairedFlowable(first=first, second=self._second)
    #
    # def match(self, *others: 'PairedFlowable') -> 'PairedFlowable':
    #     first = self._first.match(*(source.main_source for source in others))
    #     second = self._first.merge(*(source.collected_source for source in others))
    #     return PairedFlowable(first=first, second=second)
    #
    # def merge(self, *others: 'PairedFlowable') -> 'PairedFlowable':
    #     first = self._first.merge(*(source.main_source for source in others))
    #     second = self._first.merge(*(source.collected_source for source in others))
    #     return PairedFlowable(first=first, second=second)
    #
    # def observe_on(self, scheduler: Scheduler) -> 'PairedFlowable':
    #     pass
    #
    # def pairwise(self) -> 'PairedFlowable':
    #     first = self._first.pairwise()
    #     return PairedFlowable(first=first, second=self._second)
    #
    # def pipe(self, *operators: Callable[[FlowableOpMixin], FlowableOpMixin]) -> 'PairedFlowable':
    #     return pipe(*operators)(self)
    #
    # def repeat_first(self) -> 'PairedFlowable':
    #     first = self._first.repeat_first()
    #     return PairedFlowable(first=first, second=self._second)
    #
    # def scan(self, func: Callable[[Any, Any], Any], initial: Any) -> 'PairedFlowable':
    #     first = self._first.scan(func=func, initial=initial)
    #     return PairedFlowable(first=first, second=self._second)
    #
    # def set_base(self, val: Base) -> 'PairedFlowable':
    #     first = self._first.set_base(val=val)
    #     return PairedFlowable(first=first, second=self._second)
    #
    # def to_list(self) -> 'PairedFlowable':
    #     first = self._first.to_list()
    #     return PairedFlowable(first=first, second=self._second)
    #
    # def collect_flowables(self, *others: 'PairedFlowable') -> 'PairedFlowable':
    #     first = self._first.collect_flowables(*(source.main_source for source in others))
    #     second = self._first.merge(*(source.collected_source for source in others))
    #     return PairedFlowable(first=first, second=second)
    #
    # def zip_with_index(self) -> 'PairedFlowable':
    #     first = self._first.zip_with_index()
    #     return PairedFlowable(first=first, second=self._second)
