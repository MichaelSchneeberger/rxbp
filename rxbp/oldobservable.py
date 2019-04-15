from typing import Callable, Any

from rx import AnonymousObservable

import rxbp

from rxbp.ack import Continue
from rxbp.observables.controlledzip import controlled_zip
from rxbp.observables.window import window
from rxbp.pipe import pipe
from rxbp.schedulers.currentthreadscheduler import current_thread_scheduler
from rxbp.scheduler import SchedulerBase, Scheduler
from rxbp.observable import Observable
from rxbp.observer import Observer


# class Observable2(ObservableBase):
#     def __init__(self, observable):
#         super().__init__(base=observable.base, transformations=observable.transformations)
#
#         self.observable = observable
#
#     def unsafe_subscribe(self, observer: Observer, scheduler: SchedulerBase,
#                          subscribe_scheduler: SchedulerBase):
#         return self.observable.unsafe_subscribe(observer, scheduler, subscribe_scheduler)
#
#     def buffer(self, size: int):
#         observable = rxbp.op.buffer(size=size)(self)
#         return Observable(observable)
#
#     def cache(self):
#         """ Converts this observable into a multicast observable that caches the items that the fastest observer has
#         already received and the slowest observer has not yet requested. Note that this observable is subscribed when
#         the multicast observable is subscribed for the first time. Therefore, this observable is never subscribed more
#         than once.
#
#         :return: multicast observable
#         """
#
#         observable = rxbp.op.cache()(self)
#         return Observable(observable)
#
#     def controlled_zip(self, right: ObservableBase, is_lower, is_higher):
#
#         o1, o2, o3 = controlled_zip(self, right, is_lower, is_higher)
#         # return Observable(o1), Observable(o2), Observable(o3)
#         return Observable(o1).buffer(size=1), Observable(o2).buffer(size=1), Observable(o3).buffer(size=1)
#
#     def flat_map(self, selector: Callable[[Any], ObservableBase]):
#         """ Applies a function to each item emitted by the source and flattens the result. The function takes any type
#         as input and returns an inner observable. The resulting observable concatenates the items of each inner
#         observable.
#
#         :param selector: A function that takes any type as input and returns an observable.
#         :return: a flattened observable
#         """
#
#         observable = rxbp.op.flat_map(selector=selector)(self)
#         return Observable(observable)
#
#     def debug(self, name=None, on_next=None, on_subscribe=None, on_ack=None, on_raw_ack=None):
#         observable = rxbp.op.debug(name=name, on_next=on_next, on_subscribe=on_subscribe, on_ack=on_ack,
#                                    on_raw_ack=on_raw_ack)(self)
#         return Observable(observable)
#
#     def execute_on(self, scheduler: Scheduler):
#         observable = rxbp.op.execute_on(scheduler=scheduler)(self)
#         return Observable(observable)
#
#     def filter(self, predicate: Callable[[Any], bool]):
#         """ Only emits those items for which the given predicate holds
#
#         :param predicate: a function that evaluates the items emitted by the source returning True if they pass the
#         filter
#         :return: filtered observable
#         """
#
#         observable = rxbp.op.filter(predicate=predicate)(self)
#         return Observable(observable)
#
#     def flat_zip(self, right: ObservableBase, inner_selector: Callable[[Any], ObservableBase],
#                  left_selector: Callable[[Any], Any]=None, result_selector: Callable[[Any, Any, Any], Any] = None):
#         observable = rxbp.op.flat_zip(right=right, inner_selector=inner_selector, left_selector=left_selector,
#                                       result_selector=result_selector)(self)
#         return Observable(observable)
#
#     def map(self, selector: Callable[[Any], Any]):
#         """ Maps each item emitted by the source by applying the given function
#
#         :param selector: function that defines the mapping
#         :return: mapped observable
#         """
#
#         observable = rxbp.op.map(selector=selector)(self)
#         return Observable(observable)
#
#     def observe_on(self, scheduler: Scheduler):
#         """ Operator that specifies a specific scheduler, on which observers will observe events
#
#         :param scheduler: a rxbackpressure scheduler
#         :return: an observable running on specified scheduler
#         """
#
#         observable = rxbp.op.observe_on(scheduler=scheduler)(self)
#         return Observable(observable)
#
#     def pairwise(self):
#         """ Creates an observable that pairs each neighbouring two items from the source
#
#         :param selector: (optional) selector function
#         :return: paired observable
#         """
#
#         observable = rxbp.op.pairwise()(self)
#         return Observable(observable)
#
#     def repeat_first(self):
#         """ Repeat the first item forever
#
#         :return:
#         """
#
#         observable = rxbp.op.repeat_first()(self)
#         return Observable(observable)
#
#     def replay(self):
#         """ Converts this observable into a multicast observable that replays the item received by the source. Note
#         that this observable is subscribed when the multicast observable is subscribed for the first time. Therefore,
#         this observable is never subscribed more than once.
#
#         :return: multicast observable
#         """
#
#         observable = rxbp.op.replay()(self)
#         return Observable(observable)
#
#     def share(self):
#         """ Converts this observable into a multicast observable that backpressures only after each subscribed
#         observer backpressures. Note that this observable is subscribed when the multicast observable is subscribed for
#         the first time. Therefore, this observable is never subscribed more than once.
#
#         :return: multicast observable
#         """
#
#         observable = rxbp.op.share()(self)
#         return Observable(observable)
#
#     def to_rx(self, scheduler=None):
#         """ Converts this observable to an rx.Observable
#
#         :param scheduler:
#         :return:
#         """
#
#         def subscribe(observer):
#             class ToRxObserver(Observer):
#
#                 def on_next(self, v):
#                     for e in v():
#                         observer.on_next(e)
#                     return Continue()
#
#                 def on_error(self, err):
#                     observer.on_error(err)
#
#                 def on_completed(self):
#                     observer.on_completed()
#
#             scheduler_ = scheduler or current_thread_scheduler
#             return self.unsafe_subscribe(ToRxObserver(), scheduler_, current_thread_scheduler)
#
#         return AnonymousObservable(subscribe)
#
#     def window(self, right: ObservableBase, is_lower, is_higher):
#         """ Forward each item from the left Observable by attaching an inner Observable to it. Subdivide or reject
#         items from the right Observable via is_lower and is_higher functions, and emit each item of a subdivision (or window)
#         in the inner Observable
#
#         :param right:
#         :param is_lower:
#         :param is_higher:
#         :return:
#         """
#
#         o1, o2 = window(self, right, is_lower, is_higher)
#         return Observable(o1).map(lambda t2: (t2[0], Observable(t2[1]).buffer(size=1))), \
#                Observable(o2).buffer(size=1)
#
#     # def controlled_zip(self, right, is_lower, is_higher, selector):
#     #     observable = ControlledZipObservable(left=self, right=right, is_lower=is_lower, is_higher=is_higher,
#     #                                          selector=selector)
#     #     return ObservableBase(observable)
#
#     def pipe(self, *operators: Callable[[ObservableBase], ObservableBase]) -> 'Observable':
#         return Observable(pipe(*operators)(self))
#
#     def zip(self, right: ObservableBase, selector: Callable[[Any, Any], Any] = None, ignore_mismatch: bool = None):
#         """ Creates a new observable from two observables by combining their item in pairs in a strict sequence.
#
#         :param selector: a mapping function applied over the generated pairs
#         :return: zipped observable
#         """
#
#         observable = rxbp.op.zip(right=right, selector=selector, ignore_mismatch=ignore_mismatch)(self)
#         return Observable(observable)
#
#     def zip_with_index(self, selector: Callable[[Any, int], Any] = None):
#         """ Zips each item emmited by the source with their indices
#
#         :param selector: a mapping function applied over the generated pairs
#         :return: zipped with index observable
#         """
#
#         observable = rxbp.op.zip_with_index(selector=selector)(self)
#         return Observable(observable)
