import functools
import itertools
from abc import ABC, abstractmethod
from dataclasses import dataclass
from traceback import FrameSummary
from typing import Callable, Any, Tuple, List

from rxbp.flowables.mapflowable import MapFlowable
from rxbp.flowables.reduceflowable import ReduceFlowable
from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.flowables.repeatfirstflowable import RepeatFirstFlowable
from rxbp.flowables.tolistflowable import ToListFlowable
from rxbp.indexed.flowables.concatindexedflowable import ConcatIndexedFlowable
from rxbp.indexed.flowables.controlledzipindexedflowable import ControlledZipIndexedFlowable
from rxbp.indexed.flowables.debugbaseindexedflowable import DebugBaseIndexedFlowable
from rxbp.indexed.flowables.filterindexedflowable import FilterIndexedFlowable
from rxbp.indexed.flowables.matchindexedflowable import MatchIndexedFlowable
from rxbp.indexed.flowables.pairwiseindexedflowable import PairwiseIndexedFlowable
from rxbp.indexed.flowables.zipindexedflowable import ZipIndexedFlowable
from rxbp.indexed.indexedsubscription import IndexedSubscription
from rxbp.indexed.mixins.indexedflowablemixin import IndexedFlowableMixin
from rxbp.indexed.selectors.flowablebase import FlowableBase
from rxbp.indexed.selectors.flowablebaseandselectors import FlowableBaseAndSelectors
from rxbp.indexed.utils.initbase import init_base
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.mixins.flowableopmixin import FlowableOpMixin
from rxbp.mixins.sharedflowablemixin import SharedFlowableMixin
from rxbp.subscriber import Subscriber


class IndexedFlowableOpMixin(
    FlowableOpMixin,
    IndexedFlowableMixin,
    ABC,
):
    @property
    @abstractmethod
    def underlying(self) -> IndexedFlowableMixin:
        ...

    def unsafe_subscribe(self, subscriber: Subscriber) -> IndexedSubscription:
        return self.underlying.unsafe_subscribe(subscriber=subscriber)

    @abstractmethod
    def _copy(self, **kwargs) -> 'IndexedFlowableOpMixin':
        ...

    def concat(self, *others: FlowableMixin) -> IndexedFlowableMixin:
        if len(others) == 0:
            return self

        all_sources = itertools.chain([self], others)
        return self._copy(underlying=ConcatIndexedFlowable(sources=list(all_sources)))

    def controlled_zip(
            self,
            right: IndexedFlowableMixin,
            stack: List[FrameSummary],
            request_left: Callable[[Any, Any], bool] = None,
            request_right: Callable[[Any, Any], bool] = None,
            match_func: Callable[[Any, Any], bool] = None,
    ) -> IndexedFlowableMixin:

        request_left = request_left if request_left is not None else lambda _, __: True
        request_right = request_right if request_right is not None else lambda _, __: True
        match_func = match_func if match_func is not None else lambda _, __: True

        flowable = ControlledZipIndexedFlowable(
            left=self,
            right=right,
            request_left=request_left,
            request_right=request_right,
            match_func=match_func,
            stack=stack,
        )

        try:
            source = next(source for source in [self, right] if isinstance(source, SharedFlowableMixin))
        except StopIteration:
            source = self

        return source._copy(underlying=flowable)

    def debug_base(
            self,
            base: Any,
            stack: List[FrameSummary],
            name: str = None,
    ):
        base = init_base(base)

        if name is None:
            name = str(base)

        return self._copy(underlying=DebugBaseIndexedFlowable(
            source=self,
            base=base,
            name=name,
            stack=stack,
        ))

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
        return self._copy(underlying=flowable)

    def match(
            self,
            *others: IndexedFlowableMixin,
            stack: List[FrameSummary],
    ) -> 'IndexedFlowableOpMixin':

        if len(others) == 0:
            return self.map(lambda v: (v,))
        else:
            sources = (self,) + others

            def gen_stack():
                for source in reversed(sources):
                    def func(right: 'IndexedFlowableOpMixin' = None, left: 'IndexedFlowableOpMixin' = source):
                        if right is None:
                            return left.map(lambda v: (v,))
                        else:
                            def inner_result_selector(t: Tuple[Any, Tuple[Any]]):
                                return (t[0],) + t[1]

                            flowable = MapFlowable(
                                source=MatchIndexedFlowable(
                                    left=left,
                                    right=right,
                                    stack=stack,
                                ),
                                func=inner_result_selector,
                            )
                            return flowable

                    yield func

            flowable: 'IndexedFlowableOpMixin' = functools.reduce(lambda acc, func: func(acc), gen_stack(), None)

            try:
                source = next(source for source in sources if isinstance(source, SharedFlowableMixin))
            except StopIteration:
                source = self

            return source._copy(underlying=flowable)

    def pairwise(self) -> IndexedFlowableMixin:

        return self._copy(underlying=PairwiseIndexedFlowable(source=self))

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
        return self._copy(underlying=flowable)

    def repeat_first(self):

        flowable = RepeatFirstFlowable(source=self)
        return self._copy(underlying=flowable)

    def _share(self, stack: List[FrameSummary]):
        return self._copy(
            underlying=RefCountFlowable(source=self, stack=stack),
            is_shared=True,
        )

    def set_base(self, val: FlowableBase):

        @dataclass
        class SetBaseIndexedFlowable(IndexedFlowableMixin):
            source: IndexedFlowableMixin
            base: FlowableBase

            def unsafe_subscribe(self, subscriber: Subscriber) -> IndexedSubscription:
                subscription = self.source.unsafe_subscribe(subscriber=subscriber)

                return subscription.copy(
                    index=FlowableBaseAndSelectors(base=self.base, selectors=None),
                    observable=subscription.observable,
                )

        return self._copy(underlying=SetBaseIndexedFlowable(
            source=self,
            base=val,
        ))

    def to_list(self):

        flowable = ToListFlowable(source=self)
        return self._copy(underlying=flowable)

    def zip(self, others: Tuple['FlowableOpMixin'], stack: List[FrameSummary]):

        if len(others) == 0:
            return self.map(lambda v: (v,))
        else:
            sources = (self,) + others

            def gen_stack():
                for source in reversed(sources):
                    def func(right: IndexedFlowableMixin = None, left: IndexedFlowableMixin = source):
                        if right is None:
                            return left.map(lambda v: (v,))
                        else:
                            def inner_result_selector(t: Tuple[Any, Tuple[Any]]):
                                return (t[0],) + t[1]

                            flowable = MapFlowable(
                                source=ZipIndexedFlowable(
                                    left=left,
                                    right=right,
                                    stack=stack,
                                ),
                                func=inner_result_selector,
                            )
                            return flowable

                    yield func

            flowable = functools.reduce(lambda acc, func: func(acc), gen_stack(), None)

            try:
                source = next(source for source in sources if isinstance(source, SharedFlowableMixin))
            except StopIteration:
                source = self

            return source._copy(underlying=flowable)
