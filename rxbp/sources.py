import functools
import itertools
from typing import Iterator, Iterable, Any, Callable, List, Tuple

import rx
from rx import operators

from rxbp.flowable import Flowable
from rxbp.flowables.concatflowable import ConcatFlowable
from rxbp.flowables.deferflowable import DeferFlowable
from rxbp.observable import Observable
from rxbp.observables.iteratorasobservable import IteratorAsObservable
from rxbp.observers.backpressurebufferedobserver import BackpressureBufferedObserver
from rxbp.observers.evictingbufferedobserver import EvictingBufferedObserver
from rxbp.observerinfo import ObserverInfo
from rxbp.overflowstrategy import OverflowStrategy, BackPressure, DropOld, ClearBuffer
from rxbp.scheduler import Scheduler
from rxbp.selectors.bases import NumericalBase, Base, ObjectRefBase
from rxbp.subscription import Subscription, SubscriptionInfo
from rxbp.subscriber import Subscriber
from rxbp.flowables.anonymousflowablebase import AnonymousFlowableBase
from rxbp.typing import ElementType, ValueType


def _from_iterator(iterator: Iterator, batch_size: int = None, base: Base = None):
    """ Converts an iterator into an observable

    :param iterator:
    :param batch_size:
    :return:
    """

    batch_size_ = batch_size or 1

    def unsafe_subscribe_func(subscriber: Subscriber) -> Subscription:
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
        return Subscription(info=SubscriptionInfo(base=base), observable=observable)

    return AnonymousFlowableBase(unsafe_subscribe_func=unsafe_subscribe_func)


def _from_iterable(iterable: Iterable, batch_size: int = None, n_elements: int = None, base: Base = None):
    """ Converts an iterable into an observable

    :param iterable:
    :param batch_size:
    :return:
    """

    if base is not None:
        base = base
    elif n_elements is not None:
        base = NumericalBase(n_elements)
    else:
        base = None

    # selector_info = SelectorInfo(base=base)

    def unsafe_subscribe_func(subscriber: Subscriber) -> Subscription:
        iterator = iter(iterable)
        subscriptable = _from_iterator(iterator=iterator, batch_size=batch_size, base=base)
        subscription = subscriptable.unsafe_subscribe(subscriber)
        return subscription

    return Flowable(AnonymousFlowableBase(unsafe_subscribe_func))


def concat(sources: Iterable[Base]):
    return Flowable(ConcatFlowable(sources=sources))


def defer(func: Callable[[Flowable], Base],
          initial: Any,
          defer_selector: Callable[[Flowable], Base] = None,
          base: Base = None):

    def lifted_func(f: FlowableBase):
        result = func(Flowable(f))
        return result

    return Flowable(DeferFlowable(
        base=base,
        func=lifted_func,
        initial=initial,
        defer_selector=defer_selector,
    ))


def from_iterable(iterable: Iterable, batch_size: int = None, base: Base = None):
    return _from_iterable(iterable=iterable, batch_size=batch_size, base=base)


def from_range(arg1: int, arg2: int = None, batch_size: int = None):
    if arg2 is None:
        start = 0
        stop = arg1
    else:
        start = arg1
        stop = arg2

    return _from_iterable(iterable=range(start, stop), batch_size=batch_size, n_elements=stop-start)


def from_list(buffer: List, batch_size: int = None):
    return _from_iterable(iterable=buffer, batch_size=batch_size, n_elements=len(buffer))


def from_rx(source: rx.Observable, batch_size: int = None, overflow_strategy: OverflowStrategy = None,
            base: Any = None):
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
                def iterable_to_gen(v: ValueType) -> ElementType:
                    def gen():
                        yield from v

                    return gen

                if isinstance(overflow_strategy, DropOld) or isinstance(overflow_strategy, ClearBuffer):
                    subscriber = EvictingBufferedObserver(observer=observer, scheduler=self.scheduler,
                                                          subscribe_scheduler=self.subscribe_scheduler,
                                                          strategy=overflow_strategy)
                else:
                    if overflow_strategy is None:
                        buffer_size = 1000
                    elif isinstance(overflow_strategy, BackPressure):
                        buffer_size = overflow_strategy.buffer_size
                    else:
                        raise AssertionError('only BackPressure is currently supported as overflow strategy')

                    subscriber = BackpressureBufferedObserver(underlying=observer, scheduler=self.scheduler,
                                                              subscribe_scheduler=self.subscribe_scheduler,
                                                              buffer_size=buffer_size)

                disposable = source.pipe(operators.buffer_with_count(batch_size_), operators.map(iterable_to_gen)) \
                    .subscribe(on_next=subscriber.on_next, on_error=subscriber.on_error,
                                        on_completed=subscriber.on_completed, scheduler=self.scheduler)
                return disposable

        observable = ToBackpressureObservable(scheduler=subscriber.scheduler,
                                              subscribe_scheduler=subscriber.subscribe_scheduler)
        return observable, {}

    return AnonymousFlowableBase(unsafe_subscribe_func, base=base_)


def return_value(elem: Any):
    """ Converts an element into an observable

    :param elem: the single item sent by the observable
    :return: single item observable
    """

    return _from_iterable([elem], n_elements=1)


def empty():
    """ Converts an element into an observable

    :param elem: the single item sent by the observable
    :return: single item observable
    """

    return _from_iterable([], n_elements=1)


def share(sources: List[Flowable], func: Callable[..., Flowable]):
    def gen_stack():
        for source in reversed(sources):
            def _(func: Callable[..., Flowable], source=source):
                def __(*args):
                    def ___(f: Flowable):
                        new_args = args + (f,)
                        return func(*new_args)

                    return source.share(___)
                return __
            yield _

    __ = functools.reduce(lambda acc, _: _(acc), gen_stack(), func)
    return __()


def zip(sources: List[Flowable], result_selector: Callable[..., Any] = None) -> Flowable:
    """ Creates a new observable from two observables by combining their item in pairs in a strict sequence.

    :param selector: a mapping function applied over the generated pairs
    :return: zipped observable
    """

    assert isinstance(sources, list), 'rxbp.source.zip takes a list of Flowable sources in opposition to' \
                                      ' rxbp.op.zip that takes a single Flowable source'

    def gen_stack():
        for source in reversed(sources):
            def _(right: Flowable = None, left: Flowable = source):
                if right is None:
                    return left.map(lambda v: (v,))
                else:
                    def inner_result_selector(v1: Any, v2: Tuple[Any]):
                        return (v1,) + v2

                    return left.zip(right, selector=inner_result_selector)

            yield _

    obs = functools.reduce(lambda acc, v: v(acc), gen_stack(), None)

    if result_selector is None:
        return obs
    else:
        return obs.map(lambda t: result_selector(*t))


def merge(sources: List[Flowable]) -> Flowable:
    """
    """

    assert isinstance(sources, list), 'rxbp.source.zip takes a list of Flowable sources in opposition to' \
                                      ' rxbp.op.zip that takes a single Flowable source'

    def gen_stack():
        for source in sources:
            def _(right: Flowable = None, left: Flowable = source):
                if right is None:
                    return left
                else:
                    return left.merge(right)

            yield _

    obs = functools.reduce(lambda acc, v: v(acc), gen_stack(), None)

    return obs


def match(sources: List[Flowable], result_selector: Callable[..., Any] = None):
    """ Creates a new observable from two observables by combining their item in pairs in a strict sequence.

    :param selector: a mapping function applied over the generated pairs
    :return: zipped observable
    """

    assert isinstance(sources, list), 'rxbp.source.match takes a list of Flowable sources in opposition to' \
                                      ' rxbp.op.match that takes a single Flowable source'

    def gen_stack():
        for source in sources:
            def _(left: Flowable = None, right: Flowable = source):
                if left is None:
                    return right.map(lambda v: (v,))
                else:
                    def inner_result_selector(v1, v2):
                        return v1 + (v2,)

                    return left.match(right, selector=inner_result_selector)

            yield _

    obs = functools.reduce(lambda acc, v: v(acc), gen_stack(), None)

    if result_selector is None:
        return obs
    else:
        return obs.map(lambda t: result_selector(*t))
