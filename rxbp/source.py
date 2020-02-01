import math
from typing import Iterable, Any, List

import rx
from rx import operators
from rx.core.typing import Disposable

from rxbp.flowable import Flowable
from rxbp.flowablebase import FlowableBase
from rxbp.flowables.anonymousflowablebase import AnonymousFlowableBase
from rxbp.flowables.concatflowable import ConcatFlowable
from rxbp.observable import Observable
from rxbp.observables.iteratorasobservable import IteratorAsObservable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.backpressurebufferedobserver import BackpressureBufferedObserver
from rxbp.observers.evictingbufferedobserver import EvictingBufferedObserver
from rxbp.overflowstrategy import OverflowStrategy, BackPressure, DropOld, ClearBuffer
from rxbp.scheduler import Scheduler
from rxbp.selectors.base import Base
from rxbp.selectors.bases.numericalbase import NumericalBase
from rxbp.selectors.bases.objectrefbase import ObjectRefBase
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


def concat(sources: Iterable[FlowableBase]):
    return Flowable(ConcatFlowable(sources=sources))


def empty():
    """ Converts an element into an observable

    :param elem: the single item sent by the observable
    :return: single item observable
    """

    base = NumericalBase(0)

    class EmptyFlowable(FlowableBase):
        def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:

            class EmptyObservable(Observable):
                def observe(self, observer_info: ObserverInfo) -> Disposable:
                    def action(_, __):
                        observer_info.observer.on_completed()

                    return subscriber.subscribe_scheduler.schedule(action)

            return Subscription(
                info=BaseAndSelectors(
                    base=base,
                ),
                observable=EmptyObservable(),
            )

    return Flowable(EmptyFlowable())


def from_iterable(iterable: Iterable, base: Base = None):
    # return _from_iterable(iterable=iterable, batch_size=batch_size, base=base)

    if base is not None:
        if isinstance(base, str):
            base = ObjectRefBase(base)
        elif isinstance(base, int):
            base = NumericalBase(base)
        elif isinstance(base, Base):
            base = base
        else:
            raise Exception(f'illegal base "{base}"')

    else:
        base = None

    class FromIterableFlowable(FlowableBase):
        def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
            iterator = iter(iterable)

            class FromIterableObservable(Observable):
                def observe(self, observer_info: ObserverInfo) -> Disposable:

                    def action(_, __):
                        observer_info.observer.on_next(iterator)
                        observer_info.observer.on_completed()

                    return subscriber.subscribe_scheduler.schedule(action)

            return Subscription(
                info=BaseAndSelectors(
                    base=base,
                ),
                observable=FromIterableObservable(),
            )

    return Flowable(FromIterableFlowable())


def from_range(arg1: int, arg2: int = None, batch_size: int = None, base: Any = None):
    if arg2 is None:
        start_idx = 0
        stop_idx = arg1
    else:
        start_idx = arg1
        stop_idx = arg2

    n_elements = stop_idx - start_idx

    if base is not None:
        if isinstance(base, str):
            base = ObjectRefBase(base)
        elif isinstance(base, int):
            base = NumericalBase(base)
        elif isinstance(base, Base):
            base = base
        else:
            raise Exception(f'illegal base "{base}"')
    elif n_elements is not None:
        base = NumericalBase(n_elements)
    else:
        base = None

    if batch_size is None:
        class FromRangeIterable:
            def __iter__(self):
                return iter(range(start_idx, stop_idx))

        return from_iterable(
            iterable=FromRangeIterable(),
            base=base,
        )

    else:
        n_batches = max(math.ceil(n_elements / batch_size) - 1, 0)

        def gen_iterator():
            current_start_idx = start_idx
            current_stop_idx = start_idx

            for idx in range(n_batches):
                current_stop_idx = current_stop_idx + batch_size

                yield range(current_start_idx, current_stop_idx)

                current_start_idx = current_stop_idx

            yield range(current_start_idx, stop_idx)

        class FromIterableFlowable(FlowableBase):
            def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
                iterator = gen_iterator()

                observable = IteratorAsObservable(
                    iterator=iterator,
                    scheduler=subscriber.scheduler,
                    subscribe_scheduler=subscriber.subscribe_scheduler,
                )

                return Subscription(
                    info=BaseAndSelectors(
                        base=base,
                    ),
                    observable=observable,
                )

        return Flowable(FromIterableFlowable())


def from_list(buffer: List, batch_size: int = None):
    # return _from_iterable(iterable=buffer, batch_size=batch_size, n_elements=len(buffer))

    base = NumericalBase(len(buffer))

    def unsafe_subscribe_func(subscriber: Subscriber) -> Subscription:

        if batch_size is None or batch_size == 1:
            iterator = ([e] for e in buffer)
        else:
            n_full_slices = int(len(buffer) / batch_size)

            def gen():
                idx = 0
                for _ in range(n_full_slices):
                    next_idx = idx + batch_size
                    yield buffer[idx:next_idx]
                    idx = next_idx
                yield buffer[idx:]

            iterator = gen()

        observable = IteratorAsObservable(iterator=iterator, scheduler=subscriber.scheduler,
                                          subscribe_scheduler=subscriber.subscribe_scheduler)

        return Subscription(
            info=BaseAndSelectors(base=base),
            observable=observable,
        )

    return Flowable(AnonymousFlowableBase(unsafe_subscribe_func=unsafe_subscribe_func))


def from_rx(source: rx.Observable, batch_size: int = None, overflow_strategy: OverflowStrategy = None,
            base: Any = None) -> Flowable:
    """ Wraps a rx.Observable and exposes it as a Flowable, relaying signals in a backpressure-aware manner.

    :param source: a rx.observable
    :param batch_size: defines the number of values send in a batch via `on_next` method
    :param overflow_strategy: defines which batches are ignored once the buffer is full
    :param base:
    :return:
    """

    batch_size_ = batch_size or 1

    if isinstance(base, Base):
        base_ = base
    elif base is not None:
        base_ = ObjectRefBase(base)
    else:
        base_ = ObjectRefBase(source)

    def unsafe_subscribe_func(subscriber: Subscriber) -> Subscription:
        class ToBackpressureObservable(Observable):
            def __init__(self, scheduler: Scheduler, subscribe_scheduler: Scheduler):
                self.scheduler = scheduler
                self.subscribe_scheduler = subscribe_scheduler

            def observe(self, observer_info: ObserverInfo):
                observer = observer_info.observer

                if isinstance(overflow_strategy, DropOld) or isinstance(overflow_strategy, ClearBuffer):
                    rx_observer = EvictingBufferedObserver(observer=observer, scheduler=self.scheduler,
                                                          subscribe_scheduler=self.subscribe_scheduler,
                                                          strategy=overflow_strategy)
                else:
                    if overflow_strategy is None:
                        buffer_size = 1000
                    elif isinstance(overflow_strategy, BackPressure):
                        buffer_size = overflow_strategy.buffer_size
                    else:
                        raise AssertionError('only BackPressure is currently supported as overflow strategy')

                    rx_observer = BackpressureBufferedObserver(
                        underlying=observer,
                        scheduler=self.scheduler,
                        subscribe_scheduler=self.subscribe_scheduler,
                        buffer_size=buffer_size,
                    )

                disposable = source.pipe(
                    operators.buffer_with_count(batch_size_),
                ).subscribe(on_next=rx_observer.on_next, on_error=rx_observer.on_error,
                            on_completed=rx_observer.on_completed, scheduler=self.scheduler)
                return disposable

        observable = ToBackpressureObservable(scheduler=subscriber.scheduler,
                                              subscribe_scheduler=subscriber.subscribe_scheduler)
        return Subscription(
            info=BaseAndSelectors(base=base_),
            observable=observable,
        )

    return Flowable(AnonymousFlowableBase(unsafe_subscribe_func))


def return_value(elem: Any):
    """ Converts an element into an observable

    :param elem: the single item sent by the observable
    :return: single item observable
    """

    base = NumericalBase(1)

    class EmptyFlowable(FlowableBase):
        def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:

            class EmptyObservable(Observable):
                def observe(self, observer_info: ObserverInfo) -> Disposable:
                    def action(_, __):
                        observer_info.observer.on_next([elem])
                        observer_info.observer.on_completed()

                    return subscriber.subscribe_scheduler.schedule(action)

            return Subscription(
                info=BaseAndSelectors(
                    base=base,
                ),
                observable=EmptyObservable(),
            )

    return Flowable(EmptyFlowable())


def merge(*sources: Flowable) -> Flowable:
    """
    """

    if len(sources) == 0:
        return empty()
    else:
        return sources[0].merge(*sources[1:])


def match(*sources: Flowable) -> Flowable:
    """ Creates a new observable from two observables by combining their item in pairs in a strict sequence.

    :param selector: a mapping function applied over the generated pairs
    :return: zipped observable
    """

    if len(sources) == 0:
        return empty()
    else:
        return sources[0].match(*sources[1:])


def zip(*sources: Flowable) -> Flowable: #, result_selector: Callable[..., Any] = None) -> Flowable:
    """ Creates a new observable from two observables by combining their item in pairs in a strict sequence.

    :param selector: a mapping function applied over the generated pairs
    :return: zipped observable
    """

    if len(sources) == 0:
        return empty()
    else:
        return sources[0].zip(*sources[1:])
