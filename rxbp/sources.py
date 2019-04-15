import itertools
from typing import List, Iterator, Iterable, Optional, Any

from rxbp.observable import Observable
from rxbp.observables.iteratorasobservable import IteratorAsObservable
from rxbp.observables.nowobservable import NowObservable
from rxbp.observers.bufferedsubscriber import BufferedSubscriber
from rxbp.subscriber import Subscriber
from rxbp.subscriptablebase import SubscriptableBase
from rxbp.subscriptables.anonymoussubscriptable import AnonymousSubscriptable


def _from_iterator(iterator: Iterator, batch_size: int = None, base: Any = None): # todo: remove n_elements?
    """ Converts an iterator into an observable

    :param iterator:
    :param batch_size:
    :return:
    """

    batch_size_ = batch_size or 1

    # if base is None:
    #     bases = None
    #     selectors = None
    # else:
    #     bases = {base}
    #     selectors = {base: None}

    def unsafe_subscribe_func(subscriber: Subscriber) -> SubscriptableBase.SubscribeReturnType:



        def gen():
            for peak_first in iterator:
                def generate_batch():
                    yield peak_first
                    for more in itertools.islice(iterator, batch_size_ - 1):
                        yield more

                # generate buffer in memory
                buffer = list(generate_batch())

                def gen_result(buffer=buffer):
                    for e in buffer:
                        yield e

                yield gen_result

        observable = IteratorAsObservable(iterator=gen(), scheduler=subscriber.scheduler,
                                          subscribe_scheduler=subscriber.subscribe_scheduler)
        return observable, {}

    return AnonymousSubscriptable(unsafe_subscribe_func=unsafe_subscribe_func, base=base)


def _from_iterable(iterable: Iterable, batch_size: int = None, n_elements: int = None):
    """ Converts an iterable into an observable

    :param iterable:
    :param batch_size:
    :return:
    """

    # bases = None if n_elements is None else {n_elements}

    def unsafe_subscribe_func(subscriber: Subscriber) -> SubscriptableBase.SubscribeReturnType:
        iterator = iter(iterable)
        subscriptable = _from_iterator(iterator=iterator, batch_size=batch_size, base=n_elements)
        source_observable, source_selectors = subscriptable.unsafe_subscribe(subscriber)
        return source_observable, source_selectors

    return AnonymousSubscriptable(unsafe_subscribe_func, base=n_elements)


def from_iterable(iterable: Iterable, batch_size: int = None):
    return _from_iterable(iterable=iterable, batch_size=batch_size)


from_ = from_iterable


def from_range(arg1: int, arg2: int = None, batch_size: int = None):
    if arg2 is None:
        start = 0
        stop = arg1
    else:
        start = arg1
        stop = arg2

    return _from_iterable(iterable=range(start, stop), batch_size=batch_size, n_elements=stop-start)


# def from_list(buffer: List, batch_size: int = 1):
#
#     def chunks():
#         """Yield successive n-sized chunks from l."""
#         for i in range(0, len(buffer), batch_size):
#             def chunk_gen(i=i):
#                 for e in buffer[i:i + batch_size]:
#                     yield e
#
#             yield chunk_gen
#
#     class ToIterableObservable(ObservableBase):
#         def __init__(self):
#             super().__init__(base=len(buffer), transformations=None)
#
#         def unsafe_subscribe(self, observer, scheduler, subscribe_scheduler):
#             iterator = iter(chunks())
#             from_iterator_obs = IteratorAsObservable(iterator=iterator)
#             disposable = from_iterator_obs.unsafe_subscribe(observer, scheduler, subscribe_scheduler)
#             return disposable
#
#     return Observable(ToIterableObservable())
#
#
# def from_rx(self, batch_size: int = 1):
#     source = self
#
#     class ToBackpressureObservable(ObservableBase):
#
#         def unsafe_subscribe(self, observer, scheduler, subscribe_scheduler):
#             def iterable_to_gen(v, _):
#                 def gen():
#                     yield from v
#                 return gen
#
#             subscriber = BufferedSubscriber(observer, scheduler, 1000)
#             disposable = source.buffer_with_count(batch_size) \
#                 .map(iterable_to_gen) \
#                 .subscribe_observer(on_next=subscriber.on_next, on_error=subscriber.on_error,
#                                     on_completed=subscriber.on_completed)
#             return disposable
#
#     return Observable(ToBackpressureObservable())
#
#
# def now(elem):
#     """ Converts an element into an observable
#
#     :param elem: the single item sent by the observable
#     :return: single item observable
#     """
#     observable = NowObservable(elem=elem)
#     return Observable(observable)
#
#
# just = now
# return_value = now
#
#
# def zip(left: Observable, right: Observable):
#     """ Creates a new observable from two observables by combining their item in pairs in a strict sequence.
#
#     :param selector: a mapping function applied over the generated pairs
#     :return: zipped observable
#     """
#
#     return left.zip(right=right)
