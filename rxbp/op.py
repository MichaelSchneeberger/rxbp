from typing import Any, Callable, Dict

from rxbp.internal.indexingop import merge_indexes, index_observable
from rxbp.observable import Observable
from rxbp.observables.connectableobservable import ConnectableObservable
from rxbp.observables.filterobservable import FilterObservable
from rxbp.observables.flatmapobservable import FlatMapObservable
from rxbp.observables.flatzipobservable import FlatZipObservable
from rxbp.observables.mapobservable import MapObservable
from rxbp.observables.observeonobservable import ObserveOnObservable
from rxbp.observables.pairwiseobservable import PairwiseObservable
from rxbp.observables.repeatfirstobservable import RepeatFirstObservable
from rxbp.observables.zip2observable import Zip2Observable
from rxbp.observables.zipwithindexobservable import ZipWithIndexObservable
from rxbp.observers.bufferedsubscriber import BufferedSubscriber
from rxbp.scheduler import Scheduler
from rxbp.subjects.cachedservefirstsubject import CachedServeFirstSubject
from rxbp.subjects.publishsubject import PublishSubject
from rxbp.subjects.replaysubject import ReplaySubject
from rxbp.subscriber import Subscriber
from rxbp.subscriptablebase import SubscriptableBase
from rxbp.subscriptableoperator import SubscriptableOperator
from rxbp.subscriptables.anonymoussubscriptable import AnonymousSubscriptable
from rxbp.subscriptables.oldliftobservablesubscriptable import LiftObservableSubscriptable, Lift2ObservableSubscriptable
from rxbp.testing.debugobservable import DebugObservable


# def buffer(size: int):
#     def func(obs: ObservableBase):
#         class BufferObservable(ObservableBase):
#             def unsafe_subscribe(self, observer, scheduler, subscribe_scheduler):
#                 buffered_subscriber = BufferedSubscriber(
#                     observer=observer, scheduler=scheduler, buffer_size=size)
#                 disposable = obs.unsafe_subscribe(buffered_subscriber, scheduler, subscribe_scheduler)
#                 return disposable
#         return BufferObservable()
#     return ObservableOperator(func)
#
#
# def cache():
#     """ Converts this observable into a multicast observable that caches the items that the fastest observer has
#     already received and the slowest observer has not yet requested. Note that this observable is subscribed when
#     the multicast observable is subscribed for the first time. Therefore, this observable is never subscribed more
#     than once.
#
#     :return: multicast observable
#     """
#
#     def func(obs: ObservableBase):
#         return ConnectableObservable(source=obs, subject=CachedServeFirstSubject()).ref_count()
#     return ObservableOperator(func)
#
#
# def debug(name=None, on_next=None, on_subscribe=None, on_ack=None, on_raw_ack=None, on_ack_msg=None):
#     def func(obs: ObservableBase):
#         return DebugObservable(source=obs, name=name, on_next=on_next, on_subscribe=on_subscribe, on_ack=on_ack,
#                                  on_raw_ack=on_raw_ack)
#     return ObservableOperator(func)
#
#
# def execute_on(scheduler: Scheduler):
#     def func(obs: ObservableBase):
#         class ExecuteOnObservable(ObservableBase):
#             def unsafe_subscribe(self, observer, _, subscribe_scheduler):
#                 disposable = obs.unsafe_subscribe(observer, scheduler, subscribe_scheduler)
#                 return disposable
#
#         return ExecuteOnObservable()
#     return ObservableOperator(func)
#

def filter(predicate: Callable[[Any], bool]):
    """ Only emits those items for which the given predicate holds

    :param predicate: a function that evaluates the items emitted by the source returning True if they pass the
    filter
    :return: filtered observable
    """

    def func(source_subscriptable: SubscriptableBase) -> SubscriptableBase:
        def unsafe_subscribe_func(subscriber: Subscriber) -> SubscriptableBase.SubscribeReturnType:
            source_observable, source_selectors = source_subscriptable.unsafe_subscribe(subscriber)

            observable = FilterObservable(source=source_observable, predicate=predicate, scheduler=subscriber.scheduler)

            # apply filter selector to each selector
            def gen_merged_selector():
                for base, indexing in source_selectors.items():
                    yield base, merge_indexes(indexing, observable.selector, subscribe_scheduler=subscriber.subscribe_scheduler, scheduler=subscriber.scheduler)

            selectors = dict(gen_merged_selector())

            if source_subscriptable.base is not None:
                selectors_ = {**selectors, **{source_subscriptable.base: observable.selector}}
            else:
                selectors_ = selectors

            return observable, selectors_

        if source_subscriptable.base is None:
            selectable_bases = source_subscriptable.selectable_bases
        else:
            selectable_bases = source_subscriptable.selectable_bases | {source_subscriptable.base}

        return AnonymousSubscriptable(unsafe_subscribe_func=unsafe_subscribe_func, base=None,
                                      selectable_bases=selectable_bases)
    return SubscriptableOperator(func)

#
# def flat_map(selector: Callable[[Any], ObservableBase]):
#     """ Applies a function to each item emitted by the source and flattens the result. The function takes any type
#     as input and returns an inner observable. The resulting observable concatenates the items of each inner
#     observable.
#
#     :param selector: A function that takes any type as input and returns an observable.
#     :return: a flattened observable
#     """
#
#     def func(obs: ObservableBase):
#         return FlatMapObservable(source=obs, selector=selector)
#     return ObservableOperator(func)
#
#
# def flat_zip(right: ObservableBase, inner_selector: Callable[[Any], ObservableBase], left_selector: Callable[[Any], Any]=None,
#              result_selector: Callable[[Any, Any, Any], Any] = None):
#     def func(obs: ObservableBase):
#         return FlatZipObservable(left=obs, right=right,
#                                  inner_selector=inner_selector, left_selector=left_selector,
#                                  result_selector=result_selector)
#     return ObservableOperator(func)


def map(selector: Callable[[Any], Any]):
    """ Maps each item emitted by the source by applying the given function

    :param selector: function that defines the mapping
    :return: mapped observable
    """

    def func(source: SubscriptableBase) -> SubscriptableBase:
        def unsafe_subscribe_func(subscriber: Subscriber) -> SubscriptableBase.SubscribeReturnType:
            source_observable, source_selectors = source.unsafe_subscribe(subscriber=subscriber)
            obs = MapObservable(source=source_observable, selector=selector)
            return obs, source_selectors

        return AnonymousSubscriptable(unsafe_subscribe_func=unsafe_subscribe_func, base=source.base,
                                      selectable_bases=source.selectable_bases)
    return SubscriptableOperator(func)


# def observe_on(scheduler: Scheduler):
#     """ Operator that specifies a specific scheduler, on which observers will observe events
#
#     :param scheduler: a rxbackpressure scheduler
#     :return: an observable running on specified scheduler
#     """
#
#     def func(obs: ObservableBase):
#         return ObserveOnObservable(source=obs, scheduler=scheduler)
#     return ObservableOperator(func)
#
#
# def pairwise():
#     """ Creates an observable that pairs each neighbouring two items from the source
#
#     :param selector: (optional) selector function
#     :return: paired observable
#     """
#
#     def func(obs: ObservableBase):
#         return PairwiseObservable(source=obs)
#     return ObservableOperator(func)
#
#
# def repeat_first():
#     """ Repeat the first item forever
#
#     :return:
#     """
#
#     def func(obs: ObservableBase):
#         return RepeatFirstObservable(source=obs)
#     return ObservableOperator(func)
#
#
# def replay():
#     """ Converts this observable into a multicast observable that replays the item received by the source. Note
#     that this observable is subscribed when the multicast observable is subscribed for the first time. Therefore,
#     this observable is never subscribed more than once.
#
#     :return: multicast observable
#     """
#
#     def func(obs: ObservableBase):
#         observable = ConnectableObservable(
#             source=obs,
#             subject=ReplaySubject()
#         ).ref_count()
#         return observable
#     return ObservableOperator(func)
#
#
# def share():
#     """ Converts this observable into a multicast observable that backpressures only after each subscribed
#     observer backpressures. Note that this observable is subscribed when the multicast observable is subscribed for
#     the first time. Therefore, this observable is never subscribed more than once.
#
#     :return: multicast observable
#     """
#
#     def func(obs: ObservableBase):
#         return ConnectableObservable(source=obs, subject=PublishSubject()).ref_count()
#     return ObservableOperator(func)


def zip(right: SubscriptableBase, selector: Callable[[Any, Any], Any] = None, ignore_mismatch: bool = None):
    """ Creates a new observable from two observables by combining their item in pairs in a strict sequence.

    :param selector: a mapping function applied over the generated pairs
    :return: zipped observable
    """

    source_right = right

    def func(source_left: SubscriptableBase) -> SubscriptableBase:
        # print(source_left.base)
        # print(source_right.base)
        # print(source_left.selectable_bases)

        selectable_bases = set()

        if ignore_mismatch is not True:
            if source_left.base is not None and source_left.base == source_right.base:
                transform_left = False
                transform_left = False

                selectable_bases = source_left.selectable_bases | source_right.selectable_bases

            elif source_left.base is not None and source_left.base in source_right.selectable_bases:
                transform_left = False
                transform_right = True

                selectable_bases = source_right.selectable_bases

            elif source_right.base is not None and source_right.base in source_left.selectable_bases:
                transform_left = False
                transform_right = True

                selectable_bases = source_left.selectable_bases

            else:

                raise AssertionError('flowable do not match')

        else:
            transform_left = False
            transform_right = False

        def unsafe_subscribe_func(subscriber: Subscriber) -> SubscriptableBase.SubscribeReturnType:
            left_obs, left_selectors = source_left.unsafe_subscribe(subscriber=subscriber)
            right_obs, right_selectors = source_right.unsafe_subscribe(subscriber=subscriber)

            selectors = {}

            if transform_left:
                index_obs = right_selectors[source_left.base]
                left_obs_ = index_observable(left_obs, index_obs)
            else:
                selectors = {**selectors, **left_selectors}
                left_obs_ = left_obs

            if transform_right:
                index_obs = left_selectors[source_right.base]
                # right_obs_ = DebugObservable(index_observable(DebugObservable(right_obs, 'd2'), DebugObservable(index_obs, 'd3')), 'd1')
                right_obs_ = index_observable(right_obs, index_obs)
            else:
                selectors = {**selectors, **right_selectors}
                right_obs_ = right_obs

            obs = Zip2Observable(left=left_obs_, right=right_obs_, selector=selector)
            return obs, selectors

        return AnonymousSubscriptable(unsafe_subscribe_func=unsafe_subscribe_func, selectable_bases=selectable_bases)
    return SubscriptableOperator(func)


def zip_with_index(selector: Callable[[Any, int], Any] = None):
    """ Zips each item emmited by the source with their indices

    :param selector: a mapping function applied over the generated pairs
    :return: zipped with index observable
    """

    def func(source: SubscriptableBase):
        def map_observable(obs: Observable):
            obs = ZipWithIndexObservable(source=obs, selector=selector)
            return obs
        return LiftObservableSubscriptable(source=source, func=map_observable)
    return SubscriptableOperator(func)
