import itertools
from typing import Iterator, Iterable, Any

import rx
from rx import operators

from rxbp.flowable import Flowable
from rxbp.observable import Observable
from rxbp.observables.iteratorasobservable import IteratorAsObservable
from rxbp.observables.nowobservable import NowObservable
from rxbp.observers.bufferedsubscriber import BufferedSubscriber
from rxbp.overflowstrategy import OverflowStrategy, BackPressure
from rxbp.scheduler import Scheduler
from rxbp.selectors.bases import NumericalBase, Base, ObjectRefBase
from rxbp.subscriber import Subscriber
from rxbp.flowablebase import FlowableBase
from rxbp.flowables.anonymousflowable import AnonymousFlowable
from rxbp.typing import ElementType, ValueType


def _from_iterator(iterator: Iterator, batch_size: int = None, base: Base = None):
    """ Converts an iterator into an observable

    :param iterator:
    :param batch_size:
    :return:
    """

    batch_size_ = batch_size or 1

    def unsafe_subscribe_func(subscriber: Subscriber) -> FlowableBase.FlowableReturnType:



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

    return AnonymousFlowable(unsafe_subscribe_func=unsafe_subscribe_func, base=base)


def _from_iterable(iterable: Iterable, batch_size: int = None, n_elements: int = None):
    """ Converts an iterable into an observable

    :param iterable:
    :param batch_size:
    :return:
    """

    base = NumericalBase(n_elements)

    def unsafe_subscribe_func(subscriber: Subscriber) -> FlowableBase.FlowableReturnType:
        iterator = iter(iterable)
        subscriptable = _from_iterator(iterator=iterator, batch_size=batch_size, base=base)
        source_observable, source_selectors = subscriptable.unsafe_subscribe(subscriber)
        return source_observable, source_selectors

    return AnonymousFlowable(unsafe_subscribe_func, base=base)


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


range_ = from_range

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


def from_rx(source: rx.Observable, batch_size: int = None, overflow_strategy: OverflowStrategy = None,
            base: Base = None):
    """ Wraps a rx.Observable and exposes it as a Flowable, relaying signals in a backpressure-aware manner.

    :param source: a rx.observable
    :param batch_size: defines the number of values send in a batch via `on_next` method
    :param overflow_strategy: defines which batches are ignored once the buffer is full
    :param base:
    :return:
    """

    batch_size_ = batch_size or 1

    if overflow_strategy is None:
        buffer_size = 1000
    elif isinstance(overflow_strategy, BackPressure):
        buffer_size = overflow_strategy.buffer_size
    else:
        raise AssertionError('only BackPressure is currently supported as overflow strategy')

    if isinstance(base, Base):
        base_ = base
    elif base is not None:
        base_ = ObjectRefBase(base)
    else:
        base_ = ObjectRefBase(source)

    def unsafe_subscribe_func(subscriber: Subscriber) -> FlowableBase.FlowableReturnType:
        class ToBackpressureObservable(Observable):
            def __init__(self, scheduler: Scheduler):
                self.scheduler = scheduler

            def observe(self, observer):
                def iterable_to_gen(v: ValueType) -> ElementType:
                    def gen():
                        yield from v

                    return gen

                subscriber = BufferedSubscriber(observer, self.scheduler, buffer_size)
                disposable = source.pipe(operators.buffer_with_count(batch_size_), operators.map(iterable_to_gen)) \
                    .subscribe(on_next=subscriber.on_next, on_error=subscriber.on_error,
                                        on_completed=subscriber.on_completed)
                return disposable

        observable = ToBackpressureObservable(scheduler=subscriber.scheduler)
        return observable, {}

    return AnonymousFlowable(unsafe_subscribe_func, base=base_)


def now(elem: Any):
    """ Converts an element into an observable

    :param elem: the single item sent by the observable
    :return: single item observable
    """


    # def unsafe_subscribe_func(subscriber: Subscriber) -> FlowableBase.FlowableReturnType:
    #     observable = NowObservable(elem=elem, subscribe_scheduler=subscriber.subscribe_scheduler)
    #     return observable, {}

    return _from_iterable([elem], n_elements=1)


just = now
return_value = now


def zip(left: Flowable, right: Flowable):
    """ Creates a new observable from two observables by combining their item in pairs in a strict sequence.

    :param selector: a mapping function applied over the generated pairs
    :return: zipped observable
    """

    return left.zip(right=right)
