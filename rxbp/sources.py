import functools
import itertools
from typing import Iterator, Iterable, Any, Callable, List, Tuple, Optional, Union

import rx
from rx import operators
from rx.disposable import CompositeDisposable, Disposable, SingleAssignmentDisposable

from rxbp.flowable import Flowable
from rxbp.flowables.concatflowable import ConcatFlowable
from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.observable import Observable
from rxbp.observables.iteratorasobservable import IteratorAsObservable
from rxbp.observer import Observer
from rxbp.observers.anonymousobserver import AnonymousObserver
from rxbp.observers.backpressurebufferedobserver import BackpressureBufferedObserver
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.observers.evictingbufferedobserver import EvictingBufferedObserver
from rxbp.overflowstrategy import OverflowStrategy, BackPressure, DropOld, ClearBuffer
from rxbp.scheduler import Scheduler
from rxbp.selectors.bases import NumericalBase, Base, ObjectRefBase, SharedBase
from rxbp.subjects.cacheservefirstsubject import CacheServeFirstSubject
from rxbp.subjects.publishsubject import PublishSubject
from rxbp.subjects.replaysubject import ReplaySubject
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


def concat(sources: Iterable[FlowableBase]):
    return Flowable(ConcatFlowable(sources=sources))


def defer(func: Callable[[FlowableBase], FlowableBase],
          initial: Any,
          defer_selector: Callable[[FlowableBase], FlowableBase] = None,
          base: Base = None):

    func2_ = defer_selector or (lambda f: f)

    class DeferFlowable(FlowableBase):
        def __init__(self, base: Base):
            super().__init__()

            self._base = base

        def unsafe_subscribe(self, subscriber: Subscriber):
            class DeferObservable(Observable):
                def observe(self, observer: Observer):
                    buffer_observer.underlying = observer
                    d1 = SingleAssignmentDisposable()

                    def action(_, __):
                        def gen_initial():
                            yield initial

                        _ = buffer_observer.on_next(gen_initial)
                        _, d3 = conn_observer.connect()
                        d1.disposable = d3

                    d2 = subscriber.subscribe_scheduler.schedule(action)

                    return CompositeDisposable(d1, d2)

            source = AnonymousFlowable(lambda subscriber: (DeferObservable(), {}))
            scheduled_source = source.observe_on(scheduler=subscriber.scheduler)

            result_flowable = scheduled_source.share(lambda flowable: func(flowable))

            # def default_subject_gen(scheduler: Scheduler):
            #     return CacheServeFirstSubject(scheduler=scheduler)

            ref_count_flowable = RefCountFlowable(result_flowable) #, subject_gen=default_subject_gen)

            defer_flowable = func2_(Flowable(ref_count_flowable))
            defer_obs, selector = defer_flowable.unsafe_subscribe(subscriber)

            obs, selector = ref_count_flowable.unsafe_subscribe(subscriber)

            buffer_observer = BackpressureBufferedObserver(underlying=None,
                                                           scheduler=subscriber.scheduler,
                                                           subscribe_scheduler=subscriber.subscribe_scheduler,
                                                           buffer_size=1)

            conn_observer = ConnectableObserver(underlying=buffer_observer,
                                                scheduler=subscriber.scheduler,
                                                subscribe_scheduler=subscriber.subscribe_scheduler)

            # buffer_observer = conn_observer

            def on_next(v):
                materialize = list(v())

                def gen():
                    yield from materialize

                return conn_observer.on_next(gen)

            volatile_observer = AnonymousObserver(on_next_func=on_next,
                                                  on_error_func=conn_observer.on_error,
                                                  on_completed_func=conn_observer.on_completed,
                                                  volatile=True)

            class DeferObservable2(Observable):
                def observe(self, observer: Observer):
                    d1 = obs.observe(observer)
                    d2 = defer_obs.observe(volatile_observer)
                    return CompositeDisposable(d1, d2)

            return DeferObservable2(), selector

    return Flowable(DeferFlowable(base=base))


# def defer(func: Callable[[FlowableBase], FlowableBase],
#           initial: Any,
#           defer_selector: Callable[[FlowableBase], FlowableBase] = None,
#           base: Base = None):
#     func2_ = defer_selector or (lambda f: f)
#
#     class DeferFlowable(FlowableBase):
#         def __init__(self, base: Base):
#             super().__init__()
#
#             self._base = base
#
#         def unsafe_subscribe(self, subscriber: Subscriber):
#             class DeferObservable(Observable):
#                 def observe(self, observer: Observer):
#                     # buffer_observer = BackpressureBufferedObserver(observer,
#                     #                                     scheduler=subscriber.scheduler,
#                     #                                     buffer_size=1)
#
#                     # disposable = defer_obs.observe(buffer_observer)
#
#                     # disposable = replay_subject.observe(observer)
#
#                     buffer_observer.observer = observer
#                     _, d1 = conn_observer.connect()
#
#                     def action(_, __):
#                         def gen_initial():
#                             yield initial
#
#                         _ = buffer_observer.on_next(gen_initial)
#
#                     d2 = subscriber.subscribe_scheduler.schedule(action)
#
#                     return CompositeDisposable(d1, d2)
#
#             source = AnonymousFlowable(lambda subscriber: (DeferObservable(), {}))
#             result_flowable = func(source.observe_on(scheduler=subscriber.scheduler))
#
#             # def subject_gen(scheduler: Scheduler):
#             #     return PublishSubject(scheduler=scheduler, min_num_of_subscriber=2)
#
#             ref_count_flowable = RefCountFlowable(result_flowable)
#
#             defer_flowable = func2_(Flowable(ref_count_flowable))
#             defer_obs, selector = defer_flowable.unsafe_subscribe(subscriber)
#
#             obs, selector = ref_count_flowable.unsafe_subscribe(subscriber)
#
#
#             buffer_observer = BackpressureBufferedObserver(underlying=None,
#                                                            scheduler=subscriber.scheduler,
#                                                            subscribe_scheduler=subscriber.subscribe_scheduler,
#                                                            buffer_size=1)
#
#             conn_observer = ConnectableObserver(underlying=buffer_observer,
#                                                  scheduler=subscriber.scheduler,
#                                                  subscribe_scheduler=subscriber.subscribe_scheduler)
#
#             def on_next(v):
#                 materialize = list(v())
#
#                 def gen():
#                     yield from materialize
#
#                 return conn_observer.on_next(gen)
#
#             volatile_observer = AnonymousObserver(on_next_func=on_next,
#                                                   on_error_func=conn_observer.on_error,
#                                                   on_completed_func=conn_observer.on_completed,
#                                                   volatile=True)
#
#             class DeferObservable2(Observable):
#                 def observe(self, observer: Observer):
#                     d1 = obs.observe(observer)
#                     d2 = defer_obs.observe(volatile_observer)
#                     return CompositeDisposable(d1, d2)
#
#             return DeferObservable2(), selector
#
#     return Flowable(DeferFlowable(base=base))


def from_iterable(iterable: Iterable, batch_size: int = None):
    return _from_iterable(iterable=iterable, batch_size=batch_size)


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

    def unsafe_subscribe_func(subscriber: Subscriber) -> FlowableBase.FlowableReturnType:
        class ToBackpressureObservable(Observable):
            def __init__(self, scheduler: Scheduler, subscribe_scheduler: Scheduler):
                self.scheduler = scheduler
                self.subscribe_scheduler = subscribe_scheduler

            def observe(self, observer):
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

    return AnonymousFlowable(unsafe_subscribe_func, base=base_)


def return_value(elem: Any):
    """ Converts an element into an observable

    :param elem: the single item sent by the observable
    :return: single item observable
    """

    return _from_iterable([elem], n_elements=1)


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


def zip(sources: List[Flowable], result_selector: Callable[..., Any] = None):
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
