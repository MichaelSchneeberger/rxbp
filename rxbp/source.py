import math
from typing import Iterable, Any, List, Optional

import rx
from rx import operators
from rx.core.typing import Disposable

from rxbp.flowable import Flowable
from rxbp.flowablebase import FlowableBase
from rxbp.flowables.anonymousflowablebase import AnonymousFlowableBase
from rxbp.observable import Observable
from rxbp.observables.iteratorasobservable import IteratorAsObservable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.backpressurebufferedobserver import BackpressureBufferedObserver
from rxbp.observers.evictingbufferedobserver import EvictingBufferedObserver
from rxbp.overflowstrategy import OverflowStrategy, BackPressure, DropOld, ClearBuffer
from rxbp.scheduler import Scheduler
from rxbp.selectors.base import Base
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.selectors.bases.numericalbase import NumericalBase
from rxbp.selectors.bases.objectrefbase import ObjectRefBase
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


def _create_base(base: Optional[Any]) -> Base:
    if base is not None:
        if isinstance(base, str):
            base = ObjectRefBase(base)
        elif isinstance(base, int):
            base = NumericalBase(base)
        elif isinstance(base, Base):
            base = base
        else:
            raise Exception(f'illegal base "{base}"')

    return base


def concat(*sources: Flowable):
    """
    Concatentates Flowables sequences together by back-pressuring the tail Flowables until
    the current Flowable has completed.

    :param sources: Zero or more Flowables
    """

    if len(sources) == 0:
        return empty()
    else:
        return sources[0].concat(*sources[1:])

    # return Flowable(ConcatFlowable(sources=sources))


def empty():
    """
    create a Flowable emitting no elements
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


def from_iterable(iterable: Iterable, base: Any = None):
    """
    Create a Flowable that emits each element of the given iterable.

    An iterable cannot be sent in batches.

    :param iterable: the iterable whose elements are sent
    :param base: the base of the Flowable sequence
    """

    base = _create_base(base)

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


def from_list(val: List, batch_size: int = None, base: Any = None):
    """
    Create a Flowable that emits each element of the given list.

    :param val: the list whose elements are sent
    :param batch_size: determines the number of elements that are sent in a batch
    :param base: the base of the Flowable sequence
    """

    buffer = val
    base = _create_base(base)

    if base is None:
        base = NumericalBase(len(buffer))

    def unsafe_subscribe_func(subscriber: Subscriber) -> Subscription:

        if batch_size is None or len(buffer) == batch_size:

            class FromListObservable(Observable):
                def observe(self, observer_info: ObserverInfo) -> Disposable:
                    def action(_, __):
                        # send the entire buffer at once
                        observer_info.observer.on_next(buffer)
                        observer_info.observer.on_completed()

                    return subscriber.subscribe_scheduler.schedule(action)

            observable = FromListObservable()

        else:
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


def from_range(arg1: int, arg2: int = None, batch_size: int = None, base: Any = None):
    """
    Create a Flowable that emits elements defined by the range.

    :param arg1: start identifier
    :param arg2: end identifier
    :param batch_size: determines the number of elements that are sent in a batch
    :param base: the base of the Flowable sequence
    """

    if arg2 is None:
        start_idx = 0
        stop_idx = arg1
    else:
        start_idx = arg1
        stop_idx = arg2

    n_elements = stop_idx - start_idx

    base = _create_base(base)

    if base is None:
        base = NumericalBase(n_elements)

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


def from_rx(source: rx.Observable, batch_size: int = None, overflow_strategy: OverflowStrategy = None,
            base: Any = None, is_batched: bool = None) -> Flowable:
    """
    Wrap a rx.Observable and exposes it as a Flowable, relaying signals in a backpressure-aware manner.

    :param source: an rx.observable
    :param overflow_strategy: define which batches are ignored once the buffer is full
    :param batch_size: determines the number of elements that are sent in a batch
    :param base: the base of the Flowable sequence
    :param is_batched: if set to True, the elements emitted by the source rx.Observable are
    either of type List or of type Iterator
    """

    batch_size_ = batch_size or 1

    base = _create_base(base)

    if base is None:
        base_ = ObjectRefBase(source)
    else:
        base_ = base

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
                        buffer_size = None
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

                if is_batched is True:
                    batched_source = source
                else:
                    batched_source = source.pipe(
                        operators.buffer_with_count(batch_size_),
                    )

                disposable = batched_source.subscribe(
                    on_next=rx_observer.on_next,
                    on_error=rx_observer.on_error,
                    on_completed=rx_observer.on_completed,
                    scheduler=self.scheduler,
                )
                return disposable

        observable = ToBackpressureObservable(scheduler=subscriber.scheduler,
                                              subscribe_scheduler=subscriber.subscribe_scheduler)
        return Subscription(
            info=BaseAndSelectors(base=base_),
            observable=observable,
        )

    return Flowable(AnonymousFlowableBase(unsafe_subscribe_func))


def return_value(val: Any):
    """
    Create a Flowable that emits a single element.

    :param val: the single element emitted by the Flowable
    """

    base = NumericalBase(1)

    class ReturnValueFlowable(FlowableBase):
        def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:

            class EmptyObservable(Observable):
                def observe(self, observer_info: ObserverInfo) -> Disposable:
                    def action(_, __):
                        _ = observer_info.observer.on_next([val])
                        observer_info.observer.on_completed()

                    return subscriber.subscribe_scheduler.schedule(action)

            return Subscription(
                info=BaseAndSelectors(
                    base=base,
                ),
                observable=EmptyObservable(),
            )

    return Flowable(ReturnValueFlowable())


def match(*sources: Flowable) -> Flowable:
    """
    Create a new Flowable from zero or more Flowables by first filtering and duplicating (if necessary)
    the elements of each Flowable and zip the resulting Flowable sequences together.

    :param sources: zeros or more Flowables to be matched
    """

    if len(sources) == 0:
        return empty()
    else:
        return sources[0].match(*sources[1:])


def merge(*sources: Flowable) -> Flowable:
    """
    Merge the elements of zero or more *Flowables* into a single *Flowable*.

    :param sources: zero or more Flowables whose elements are merged
    """

    if len(sources) == 0:
        return empty()
    else:
        return sources[0].merge(*sources[1:])


def zip(*sources: Flowable) -> Flowable: #, result_selector: Callable[..., Any] = None) -> Flowable:
    """
    Create a new Flowable from zero or more Flowables by combining their item in pairs in a strict sequence.

    :param sources: zero or more Flowables whose elements are zipped together
    """

    if len(sources) == 0:
        return empty()
    else:
        return sources[0].zip(*sources[1:])
