from typing import Callable, Any, Generic

from rxbp.flowables.controlledzipflowable import ControlledZipFlowable
from rxbp.flowables.filterflowable import FilterFlowable
from rxbp.flowables.flatmapflowable import FlatMapFlowable
from rxbp.flowables.mapflowable import MapFlowable
from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.flowables.sharedflowable import SharedFlowable
from rxbp.flowables.zipflowable import ZipFlowable
from rxbp.observable import Observable
from rxbp.observables.mapobservable import MapObservable
from rxbp.subscriber import Subscriber
from rxbp.flowablebase import FlowableBase
from rxbp.typing import ValueType


class Flowable(Generic[ValueType], FlowableBase[ValueType]):
    def __init__(self, subscriptable: FlowableBase[ValueType]):
        super().__init__(base=subscriptable.base, selectable_bases=subscriptable.selectable_bases)

        self.subscriptable = subscriptable

    def unsafe_subscribe(self, subscriber: Subscriber) -> Observable:
        return self.subscriptable.unsafe_subscribe(subscriber=subscriber)

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

        flowable = MapFlowable(source=self, selector=selector)
        return Flowable(flowable)

    def share(self, func: Callable[[FlowableBase], FlowableBase]):
        def lifted_func(f: RefCountFlowable):
            return func(Flowable(f))
        
        flowable =  SharedFlowable(source=self, func=lifted_func)
        return Flowable(flowable)

    def zip(self, right: FlowableBase, selector: Callable[[Any, Any], Any] = None, auto_match: bool = None):
        """ Creates a new observable from two observables by combining their item in pairs in a strict sequence.

        :param selector: a mapping function applied over the generated pairs
        :return: zipped observable
        """

        flowable =  ZipFlowable(left=self, right=right, selector=selector, auto_match=auto_match)
        return Flowable(flowable)

    # def zip_with_index(self, selector: Callable[[Any, int], Any] = None):
    #     """ Zips each item emmited by the source with their indices
    #
    #     :param selector: a mapping function applied over the generated pairs
    #     :return: zipped with index observable
    #     """
    #
    #     observable = rxbp.op.zip_with_index(selector=selector)(self)
    #     return Flowable(observable)
