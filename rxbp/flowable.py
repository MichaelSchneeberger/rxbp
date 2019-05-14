import itertools
from typing import Callable, Any, Generic, Iterator, Iterable, List, Tuple

import rx

from rxbp.flowables.cacheservefirstflowable import CacheServeFirstFlowable
from rxbp.flowables.concatflowable import ConcatFlowable
from rxbp.observables.mergeobservable import MergeObservable
from rxbp.observables.scanobservable import ScanObservable
from rxbp.pipe import pipe
from rxbp.selectors.bases import Base, PairwiseBase
from rxbp.toiterator import to_iterator
from rxbp.torx import to_rx
from rxbp.flowables.controlledzipflowable import ControlledZipFlowable
from rxbp.flowables.filterflowable import FilterFlowable
from rxbp.flowables.flatmapflowable import FlatMapFlowable
from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.flowables.shareflowable import ShareFlowable
from rxbp.flowables.zipflowable import ZipFlowable
from rxbp.observable import Observable
from rxbp.observables.mapobservable import MapObservable
from rxbp.observables.observeonobservable import ObserveOnObservable
from rxbp.observables.pairwiseobservable import PairwiseObservable
from rxbp.observables.repeatfirstobservable import RepeatFirstObservable
from rxbp.observables.zipwithindexobservable import ZipWithIndexObservable
from rxbp.scheduler import Scheduler
from rxbp.subscriber import Subscriber
from rxbp.flowablebase import FlowableBase
from rxbp.testing.debugobservable import DebugObservable
from rxbp.typing import ValueType


class Flowable(Generic[ValueType], FlowableBase[ValueType]):
    def __init__(self, flowable: FlowableBase[ValueType]):
        super().__init__(base=flowable.base, selectable_bases=flowable.selectable_bases)

        self.subscriptable = flowable

    def unsafe_subscribe(self, subscriber: Subscriber) -> Observable:
        return self.subscriptable.unsafe_subscribe(subscriber=subscriber)

    def concat(self, sources: Iterable[FlowableBase]):
        all_sources = itertools.chain([self], sources)
        flowable = ConcatFlowable(sources=all_sources)
        return Flowable(flowable)

    def controlled_zip(self, right: FlowableBase, request_left: Callable[[Any, Any], bool],
                       request_right: Callable[[Any, Any], bool],
                       match_func: Callable[[Any, Any], bool]) -> 'Flowable[ValueType]':
        """ Creates a new observable from two observables by combining their item in pairs in a strict sequence.

        :param selector: a mapping function applied over the generated pairs
        :return: zipped observable
        """

        flowable = ControlledZipFlowable(left=self, right=right, request_left=request_left,
                                            request_right=request_right,
                                            match_func=match_func)
        return Flowable(flowable)

    def debug(self, name=None, on_next=None, on_subscribe=None, on_ack=None, on_raw_ack=None, on_ack_msg=None):
        class DebugFlowable(FlowableBase):
            def __init__(self, source: FlowableBase):
                super().__init__(base=source.base, selectable_bases=source.selectable_bases)

                self._source = source

            def unsafe_subscribe(self, subscriber: Subscriber):
                source_obs, selector = self._source.unsafe_subscribe(subscriber=subscriber)

                obs = DebugObservable(source=source_obs, name=name, on_next=on_next, on_subscribe=on_subscribe,
                                      on_ack=on_ack, on_raw_ack=on_raw_ack)

                return obs, selector

        return Flowable(DebugFlowable(source=self))

    def execute_on(self, scheduler: Scheduler):
        class ExecuteOnFlowable(FlowableBase):
            def __init__(self, source: FlowableBase, scheduler: Scheduler):
                super().__init__(base=source.base, selectable_bases=source.selectable_bases)

                self._source = source
                self._scheduler = scheduler

            def unsafe_subscribe(self, subscriber: Subscriber):
                updated_subscriber = Subscriber(scheduler=self._scheduler,
                                                subscribe_scheduler=subscriber.subscribe_scheduler)

                return self._source.unsafe_subscribe(updated_subscriber)

        return Flowable(ExecuteOnFlowable(source=self, scheduler=scheduler))

    def filter(self, predicate: Callable[[Any], bool]) -> 'Flowable[ValueType]':
        """ Only emits those items for which the given predicate holds

        :param predicate: a function that evaluates the items emitted by the source returning True if they pass the
        filter
        :return: filtered observable
        """

        flowable = FilterFlowable(source=self, predicate=predicate)
        return Flowable(flowable)

    def flat_map(self, selector: Callable[[Any], FlowableBase]):
        flowable = FlatMapFlowable(source=self, selector=selector)
        return Flowable(flowable)

    def map(self, selector: Callable[[ValueType], Any]):
        """ Maps each item emitted by the source by applying the given function

        :param selector: function that defines the mapping
        :return: mapped observable
        """

        class MapFlowable(FlowableBase):
            def __init__(self, source: FlowableBase, selector: Callable[[ValueType], Any]):
                super().__init__(base=source.base, selectable_bases=source.selectable_bases)

                self._source = source
                self._selector = selector

            def unsafe_subscribe(self, subscriber: Subscriber) -> FlowableBase.FlowableReturnType:
                source_observable, source_selectors = self._source.unsafe_subscribe(subscriber=subscriber)
                obs = MapObservable(source=source_observable, selector=self._selector)
                return obs, source_selectors

        flowable = MapFlowable(source=self, selector=selector)
        return Flowable(flowable)

    def merge(self, other: FlowableBase):
        """

        :param selector: (optional) selector function
        :return: paired observable
        """

        class MergeFlowable(FlowableBase):
            def __init__(self, source: FlowableBase):
                super().__init__()

                self._source = source

            def unsafe_subscribe(self, subscriber: Subscriber):
                left_obs, _ = self._source.unsafe_subscribe(subscriber=subscriber)
                right_obs, _ = other.unsafe_subscribe(subscriber=subscriber)
                obs = MergeObservable(left=left_obs, right=right_obs)

                return obs, {}

        return Flowable(MergeFlowable(source=self))

    def observe_on(self, scheduler: Scheduler):
        """ Operator that specifies a specific scheduler, on which observers will observe events

        :param scheduler: a rxbackpressure scheduler
        :return: an observable running on specified scheduler
        """

        class ObserveOnFlowable(FlowableBase):
            def __init__(self, source: FlowableBase, scheduler: Scheduler):
                super().__init__(base=source.base, selectable_bases=source.selectable_bases)

                self._source = source
                self._scheduler = scheduler

            def unsafe_subscribe(self, subscriber: Subscriber):
                source_obs, selectors = self._source.unsafe_subscribe(subscriber=subscriber)
                obs = ObserveOnObservable(source=source_obs, scheduler=scheduler)

                return obs, selectors

        return Flowable(ObserveOnFlowable(source=self, scheduler=scheduler))

    def pairwise(self):
        """ Creates an observable that pairs each neighbouring two items from the source

        :param selector: (optional) selector function
        :return: paired observable
        """

        class PairwiseFlowable(FlowableBase):
            def __init__(self, source: FlowableBase):
                if isinstance(source.base, Base):
                    base = PairwiseBase(source.base)
                else:
                    base = None
                super().__init__(base=base)

                self._source = source

            def unsafe_subscribe(self, subscriber: Subscriber):
                source_obs, selectors = self._source.unsafe_subscribe(subscriber=subscriber)
                obs = PairwiseObservable(source=source_obs)

                return obs, selectors

        return Flowable(PairwiseFlowable(source=self))

    def pipe(self, *operators: Callable[[FlowableBase], FlowableBase]):
        return Flowable(pipe(*operators)(self))

    def run(self):
        return list(to_iterator(self))

    def repeat_first(self):
        """ Repeat the first item forever

        :return:
        """

        class RepeatFirstFlowable(FlowableBase):
            def __init__(self, source: FlowableBase):
                super().__init__()

                self._source = source

            def unsafe_subscribe(self, subscriber: Subscriber) -> FlowableBase.FlowableReturnType:
                source_observable, source_selectors = self._source.unsafe_subscribe(subscriber=subscriber)
                obs = RepeatFirstObservable(source=source_observable, scheduler=subscriber.scheduler)
                return obs, source_selectors

        flowable = RepeatFirstFlowable(source=self)
        return Flowable(flowable)

    def scan(self, func: Callable[[Any, Any], Any], initial: Any):
        source = self

        class ScanFlowable(FlowableBase):
            def __init__(self):
                super().__init__(base=source.base, selectable_bases=source.selectable_bases)

            def unsafe_subscribe(self, subscriber: Subscriber) -> FlowableBase.FlowableReturnType:
                source_observable, source_selectors = source.unsafe_subscribe(subscriber=subscriber)
                obs = ScanObservable(source=source_observable, func=func, initial=initial)
                return obs, source_selectors

        flowable = ScanFlowable()
        return Flowable(flowable)

    def share(self, func: Callable[[FlowableBase], FlowableBase]):
        def lifted_func(f: RefCountFlowable):
            return func(Flowable(f))

        flowable = CacheServeFirstFlowable(source=self, func=lifted_func)
        return Flowable(flowable)

    # def share(self, func: Callable[[FlowableBase], FlowableBase]):
    #     def lifted_func(f: RefCountFlowable):
    #         return func(Flowable(f))
    #
    #     flowable = ShareFlowable(source=self, func=lifted_func)
    #     return Flowable(flowable)

    def to_rx(self) -> rx.Observable:
        """ Converts this observable to an rx.Observable

        :param scheduler:
        :return:
        """

        return to_rx(source=self)

    def match(self, right: FlowableBase, selector: Callable[[Any, Any], Any] = None):
        """ Creates a new observable from two observables by combining their item in pairs in a strict sequence.

        :param selector: a mapping function applied over the generated pairs
        :return: zipped observable
        """

        flowable =  ZipFlowable(left=self, right=right, selector=selector, auto_match=True)
        return Flowable(flowable)

    def zip(self, right: FlowableBase, selector: Callable[[Any, Any], Any] = None):
        """ Creates a new observable from two observables by combining their item in pairs in a strict sequence.

        :param selector: a mapping function applied over the generated pairs
        :return: zipped observable
        """

        flowable =  ZipFlowable(left=self, right=right, selector=selector, auto_match=False)
        return Flowable(flowable)

    def zip_with_index(self, selector: Callable[[Any, int], Any] = None):
        """ Zips each item emmited by the source with their indices

        :param selector: a mapping function applied over the generated pairs
        :return: zipped with index observable
        """

        class ZipWithIndexFlowable(FlowableBase):
            def __init__(self, source: FlowableBase, selector: Callable[[ValueType], Any]):
                super().__init__(base=source.base, selectable_bases=source.selectable_bases)

                self._source = source
                self._selector = selector

            def unsafe_subscribe_func(self, subscriber: Subscriber) -> FlowableBase.FlowableReturnType:
                source_observable, source_selectors = self._source.unsafe_subscribe(subscriber=subscriber)
                obs = ZipWithIndexObservable(source=source_observable, selector=self._selector)
                return obs, source_selectors

        flowable = ZipWithIndexFlowable(source=self, selector=selector)
        return Flowable(flowable)
