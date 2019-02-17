import itertools
from typing import List, Iterator, Iterable

from rxbp.observable import Observable
from rxbp.observablebase import ObservableBase
from rxbp.observables.iteratorasobservable import IteratorAsObservable
from rxbp.observables.nowobservable import NowObservable
from rxbp.observers.bufferedsubscriber import BufferedSubscriber


def from_(iterable: Iterable, batch_size: int = 1):
    """ Converts an iterable into an observable

    :param iterable:
    :param batch_size:
    :return:
    """

    class ToIterableObservable(Observable):

        def unsafe_subscribe(self, observer, scheduler, subscribe_scheduler):
            iterator = iter(iterable)
            from_iterator_obs = from_iterator(iterator=iterator, batch_size=batch_size)
            disposable = from_iterator_obs.unsafe_subscribe(observer, scheduler, subscribe_scheduler)
            return disposable

    return ObservableBase(ToIterableObservable())


from_iterable = from_


def from_iterator(iterator: Iterator, batch_size: int = 1):
    """ Converts an iterator into an observable

    :param iterator:
    :param batch_size:
    :return:
    """

    def gen():
        for peak_first in iterator:
            def generate_batch():
                yield peak_first
                for more in itertools.islice(iterator, batch_size - 1):
                    yield more

            # generate buffer in memory
            buffer = list(generate_batch())

            def gen_result(buffer=buffer):
                for e in buffer:
                    yield e

            yield gen_result

    observable = IteratorAsObservable(iterator=gen())
    return ObservableBase(observable)


def from_list(buffer: List, batch_size: int = 1):

    def chunks():
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(buffer), batch_size):
            def chunk_gen(i=i):
                for e in buffer[i:i + batch_size]:
                    yield e

            yield chunk_gen

    class ToIterableObservable(Observable):

        def unsafe_subscribe(self, observer, scheduler, subscribe_scheduler):
            iterator = iter(chunks())
            from_iterator_obs = IteratorAsObservable(iterator=iterator)
            disposable = from_iterator_obs.unsafe_subscribe(observer, scheduler, subscribe_scheduler)
            return disposable

    return ObservableBase(ToIterableObservable())


def from_rx(self, batch_size: int = 1):
    source = self

    class ToBackpressureObservable(Observable):

        def unsafe_subscribe(self, observer, scheduler, subscribe_scheduler):
            def iterable_to_gen(v, _):
                def gen():
                    yield from v
                return gen

            subscriber = BufferedSubscriber(observer, scheduler, 1000)
            disposable = source.buffer_with_count(batch_size) \
                .map(iterable_to_gen) \
                .subscribe(on_next=subscriber.on_next, on_error=subscriber.on_error,
                           on_completed=subscriber.on_completed)
            return disposable

    return ObservableBase(ToBackpressureObservable())


def now(elem):
    """ Converts an element into an observable

    :param elem: the single item sent by the observable
    :return: single item observable
    """
    observable = NowObservable(elem=elem)
    return ObservableBase(observable)


just = now
return_value = now


def zip(left: ObservableBase, right: ObservableBase):
    """ Creates a new observable from two observables by combining their item in pairs in a strict sequence.

    :param selector: a mapping function applied over the generated pairs
    :return: zipped observable
    """

    return left.zip(right=right)
