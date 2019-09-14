import itertools
from typing import Callable, Any, Generic, Iterable

import rx
from rxbp.flowables.anonymousflowablebase import AnonymousFlowableBase
from rxbp.flowables.bufferflowable import BufferFlowable

from rxbp.flowables.cacheservefirstflowable import CacheServeFirstFlowable
from rxbp.flowables.concatflowable import ConcatFlowable
from rxbp.flowables.debugflowable import DebugFlowable
from rxbp.flowables.executeonflowable import ExecuteOnFlowable
from rxbp.flowables.mapflowable import MapFlowable
from rxbp.flowables.mergeflowable import MergeFlowable
from rxbp.flowables.observeonflowable import ObserveOnFlowable
from rxbp.flowables.pairwiseflowable import PairwiseFlowable
from rxbp.flowables.repeatfirstflowable import RepeatFirstFlowable
from rxbp.flowables.scanflowable import ScanFlowable
from rxbp.flowables.tolistflowable import ToListFlowable
from rxbp.flowables.zipwithindexflowable import ZipWithIndexFlowable
from rxbp.pipe import pipe
from rxbp.selectors.bases import Base
from rxbp.subscription import Subscription, SubscriptionInfo
from rxbp.toiterator import to_iterator
from rxbp.torx import to_rx
from rxbp.flowables.controlledzipflowable import ControlledZipFlowable
from rxbp.flowables.filterflowable import FilterFlowable
from rxbp.flowables.flatmapflowable import FlatMapFlowable
from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.flowables.zip2flowable import Zip2Flowable
from rxbp.observable import Observable
from rxbp.scheduler import Scheduler
from rxbp.subscriber import Subscriber
from rxbp.flowablebase import FlowableBase
from rxbp.typing import ValueType


class Flowable(Generic[ValueType], FlowableBase[ValueType]):
    def __init__(self, flowable: FlowableBase[ValueType]):
        super().__init__()

        self.subscriptable = flowable

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        return self.subscriptable.unsafe_subscribe(subscriber=subscriber)

    def buffer(self, buffer_size: int):
        flowable = BufferFlowable(source=self, buffer_size=buffer_size)
        return Flowable(flowable)

    def concat(self, sources: Iterable[FlowableBase]):
        all_sources = itertools.chain([self], sources)
        flowable = ConcatFlowable(sources=all_sources)
        return Flowable(flowable)

    def controlled_zip(self, right: FlowableBase,
                       request_left: Callable[[Any, Any], bool] = None,
                       request_right: Callable[[Any, Any], bool] = None,
                       match_func: Callable[[Any, Any], bool] = None) -> 'Flowable[ValueType]':
        """ Creates a new Flowable from two Flowables by combining their item in pairs in a strict sequence.

        :param selector: a mapping function applied over the generated pairs
        :return: zipped Flowable
        """

        flowable = ControlledZipFlowable(left=self, right=right, request_left=request_left,
                                            request_right=request_right,
                                            match_func=match_func)
        return Flowable(flowable)

    def debug(self, name=None, on_next=None, on_subscribe=None, on_ack=None, on_raw_ack=None, on_ack_msg=None):

        return Flowable(DebugFlowable(
            source=self,
            name=name,
            on_next=on_next,
            on_subscribe=on_subscribe,
            on_ack=on_ack,
            on_raw_ack=on_raw_ack,
            on_ack_msg=on_ack_msg,
        ))

    def execute_on(self, scheduler: Scheduler):

        return Flowable(ExecuteOnFlowable(source=self, scheduler=scheduler))

    def filter(self, predicate: Callable[[Any], bool]) -> 'Flowable[ValueType]':
        """ Only emits those items for which the given predicate holds

        :param predicate: a function that evaluates the items emitted by the source returning True if they pass the
        filter
        :return: filtered Flowable
        """

        flowable = FilterFlowable(source=self, predicate=predicate)
        return Flowable(flowable)

    def filter_with_index(self, predicate: Callable[[Any, int], bool]) -> 'Flowable[ValueType]':
        """ Only emits those items for which the given predicate holds

        :param predicate: a function that evaluates the items emitted by the source returning True if they pass the
        filter
        :return: filtered Flowable
        """

        flowable = self.zip_with_index() \
            .filter(lambda t2: predicate(t2[0], t2[1])) \
            .map(lambda t2: t2[0])

        return flowable

    def flat_map(self, selector: Callable[[Any], FlowableBase]):
        flowable = FlatMapFlowable(source=self, selector=selector)
        return Flowable(flowable)

    def map(self, selector: Callable[[ValueType], Any]):
        """ Maps each item emitted by the source by applying the given function

        :param selector: function that defines the mapping
        :return: mapped Flowable
        """

        flowable = MapFlowable(source=self, selector=selector)
        return Flowable(flowable)

    def match(self, right: FlowableBase, selector: Callable[[Any, Any], Any] = None) -> 'Flowable':
        """ Creates a new Flowable from two Flowables by combining their item in pairs in a strict sequence.

        :param selector: a mapping function applied over the generated pairs
        :return: matched Flowable
        """

        flowable =  Zip2Flowable(left=self, right=right, func=selector, auto_match=True)
        return Flowable(flowable)

    def merge(self, other: FlowableBase):
        """ Merges the elements of two Flowables into a single Flowable

        :param selector: (optional) selector function
        :return: merged Flowable
        """

        return Flowable(MergeFlowable(source=self, other=other))

    def observe_on(self, scheduler: Scheduler):
        """ Operator that specifies a specific scheduler, on which observers will observe events

        :param scheduler: a rxbackpressure scheduler
        :return: an Flowable running on specified scheduler
        """

        return Flowable(ObserveOnFlowable(source=self, scheduler=scheduler))

    def pairwise(self):
        """ Creates an Flowable that pairs each neighbouring two items from the source

        :param selector: (optional) selector function
        :return: paired Flowable
        """

        return Flowable(PairwiseFlowable(source=self))

    def pipe(self, *operators: Callable[[FlowableBase], FlowableBase]):
        return Flowable(pipe(*operators)(self))

    def run(self, scheduler: Scheduler = None):
        return list(to_iterator(source=self, scheduler=scheduler))

    def repeat_first(self):
        """ Repeat the first item forever

        :return:
        """

        flowable = RepeatFirstFlowable(source=self)
        return Flowable(flowable)

    def scan(self, func: Callable[[Any, Any], Any], initial: Any):
        flowable = ScanFlowable(source=self, func=func, initial=initial)
        return Flowable(flowable)

    def share(self, func: Callable[[FlowableBase], FlowableBase]):
        def lifted_func(f: RefCountFlowable):
            result = func(Flowable(f))
            return result

        flowable = CacheServeFirstFlowable(source=self, func=lifted_func)
        return Flowable(flowable)

    def use_base(self, val: Base):
        def unsafe_unsafe_subscribe(subscriber: Subscriber) -> Subscription:
            subscription = self.unsafe_subscribe(subscriber=subscriber)

            return Subscription(SubscriptionInfo(base=val), observable=subscription.observable)

        flowable = AnonymousFlowableBase(
            unsafe_subscribe_func=unsafe_unsafe_subscribe,
        )
        return Flowable(flowable)

    def to_list(self):
        flowable = ToListFlowable(source=self)
        return Flowable(flowable)

    def to_rx(self, batched: bool = None) -> rx.Observable:
        """ Converts this Flowable to an rx.Observable

        :param scheduler:
        :return:
        """

        return to_rx(source=self, batched=batched)

    def zip(self, right: FlowableBase, selector: Callable[[Any, Any], Any] = None):
        """ Creates a new Flowable from two Flowables by combining their item in pairs in a strict sequence.

        :param selector: a mapping function applied over the generated pairs
        :return: zipped Flowable
        """

        flowable =  Zip2Flowable(left=self, right=right, func=selector, auto_match=False)
        return Flowable(flowable)

    def zip_with_index(self, selector: Callable[[Any, int], Any] = None):
        """ Zips each item emmited by the source with their indices

        :param selector: a mapping function applied over the generated pairs
        :return: zipped with index Flowable
        """

        flowable = ZipWithIndexFlowable(source=self, selector=selector)
        return Flowable(flowable)
