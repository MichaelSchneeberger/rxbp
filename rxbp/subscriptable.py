from typing import Callable, Any

from rx.core import Disposable

import rxbp
from rxbp.observable import Observable
from rxbp.subscriber import Subscriber
from rxbp.subscriptablebase import SubscriptableBase


class Subscriptable(SubscriptableBase):
    def __init__(self, subscriptable: SubscriptableBase):
        super().__init__(base=subscriptable.base, selectable_bases=subscriptable.selectable_bases)

        self.subscriptable = subscriptable

    def unsafe_subscribe(self, subscriber: Subscriber) -> Observable:
        return self.subscriptable.unsafe_subscribe(subscriber=subscriber)

    def controlled_zip(self, right: SubscriptableBase, request_left: Callable[[Any, Any], bool],
                 request_right: Callable[[Any, Any], bool],
                 match_func: Callable[[Any, Any], bool]):
        """ Creates a new observable from two observables by combining their item in pairs in a strict sequence.

        :param selector: a mapping function applied over the generated pairs
        :return: zipped observable
        """

        observable = rxbp.op.controlled_zip(right=right, request_left=request_left,
                                            request_right=request_right,
                                            match_func=match_func)(self)
        return Subscriptable(observable)

    def filter(self, predicate: Callable[[Any], bool]):
        """ Only emits those items for which the given predicate holds

        :param predicate: a function that evaluates the items emitted by the source returning True if they pass the
        filter
        :return: filtered observable
        """

        observable = rxbp.op.filter(predicate=predicate)(self)
        return Subscriptable(observable)

    def map(self, selector: Callable[[Any], Any]):
        """ Maps each item emitted by the source by applying the given function

        :param selector: function that defines the mapping
        :return: mapped observable
        """

        subscriptable = rxbp.op.map(selector=selector)(self)
        return Subscriptable(subscriptable)

    def zip(self, right: SubscriptableBase, selector: Callable[[Any, Any], Any] = None, ignore_mismatch: bool = None):
        """ Creates a new observable from two observables by combining their item in pairs in a strict sequence.

        :param selector: a mapping function applied over the generated pairs
        :return: zipped observable
        """

        observable = rxbp.op.zip(right=right, selector=selector, ignore_mismatch=ignore_mismatch)(self)
        return Subscriptable(observable)

    def zip_with_index(self, selector: Callable[[Any, int], Any] = None):
        """ Zips each item emmited by the source with their indices

        :param selector: a mapping function applied over the generated pairs
        :return: zipped with index observable
        """

        observable = rxbp.op.zip_with_index(selector=selector)(self)
        return Subscriptable(observable)
