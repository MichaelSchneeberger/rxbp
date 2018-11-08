from typing import Callable, Any, Iterator, Iterable

from rx import AnonymousObservable
from rx.concurrency.schedulerbase import SchedulerBase
from rx.disposables import CompositeDisposable

from rxbackpressure.ack import Continue
from rxbackpressure.observables.RepeatFirstobservable import RepeatFirstObservable
from rxbackpressure.observers.bufferedsubscriber import BufferedSubscriber
from rxbackpressure.schedulers.currentthreadscheduler import current_thread_scheduler
from rxbackpressure.subjects.cachedservefirstsubject import CachedServeFirstSubject
from rxbackpressure.observables.concatmapobservable import ConcatMapObservable
from rxbackpressure.observables.connectableobservable import ConnectableObservable
from rxbackpressure.observables.flatzipobservable import FlatZipObservable
from rxbackpressure.subjects.publishsubject import PublishSubject
from rxbackpressure.subjects.replaysubject import ReplaySubject
from rxbackpressure.testing.debugobservable import DebugObservable
from rxbackpressure.observables.filterobservable import FilterObservable
from rxbackpressure.observables.iteratorasobservable import IteratorAsObservable
from rxbackpressure.observables.nowobservable import NowObservable
from rxbackpressure.scheduler import SchedulerBase, Scheduler
from rxbackpressure.observables.window import window
from rxbackpressure.observables.zipwithindexobservable import ZipWithIndexObservable
from rxbackpressure.observables.mapobservable import MapObservable
from rxbackpressure.observable import Observable
from rxbackpressure.observables.observeonobservable import ObserveOnObservable
from rxbackpressure.observer import Observer
from rxbackpressure.observables.pairwiseobservable import PairwiseObservable
from rxbackpressure.observables.zip2observable import Zip2Observable


class ObservableOp(Observable):
    def __init__(self, observable):
        self.observable = observable

    def unsafe_subscribe(self, observer: Observer, scheduler: SchedulerBase,
                         subscribe_scheduler: SchedulerBase):
        return self.observable.unsafe_subscribe(observer, scheduler, subscribe_scheduler)

    # def buffer(self):
    #     class ToBackpressureObservable(Observable):
    #
    #         def unsafe_subscribe(self, observer, scheduler, subscribe_scheduler):
    #             subscriber = BufferedSubscriber(observer, scheduler, 1000)
    #             disposable = self.observable.unsafe_subscribe(subscriber, scheduler, subscribe_scheduler)
    #             return disposable
    #
    #     return ObservableOp(ToBackpressureObservable())

    def cache(self):
        """ Converts this observable into a multicast observable that caches the items that the fastest observer has
        already received and the slowest observer has not yet requested. Note that this observable is subscribed when
        the multicast observable is subscribed for the first time. Therefore, this observable is never subscribed more
        than once.

        :return: multicast observable
        """

        observable = ConnectableObservable(source=self, subject=CachedServeFirstSubject()).ref_count()
        return ObservableOp(observable)

    def flat_map(self, selector):
        """ Applies a function to each item emitted by the source and flattens the result. The function takes any type
        as input and returns an inner observable. The resulting observable concatenates the items of each inner
        observable.

        :param selector: A function that takes any type as input and returns an observable.
        :return: a flattened observable
        """

        observable = ConcatMapObservable(source=self,
                                         selector=selector)
        return ObservableOp(observable)

    def debug(self, name, on_next=None, on_subscribe=None, on_ack=None, print_ack=None, on_ack_msg=None):
        observable = DebugObservable(self, name=name, on_next=on_next, on_subscribe=on_subscribe, on_ack=on_ack,
                                     print_ack=print_ack, on_ack_msg=on_ack_msg)
        return ObservableOp(observable)

    def execute_on(self, scheduler: Scheduler):
        source = self

        class ExecuteOnObservable(Observable):

            def unsafe_subscribe(self, observer, _, subscribe_scheduler):
                disposable = source.unsafe_subscribe(observer, scheduler, subscribe_scheduler)
                return disposable

        return ObservableOp(ExecuteOnObservable())

    def filter(self, predicate: Callable[[Any], bool]):
        """ Only emits those items for which the given predicate holds

        :param predicate: a function that evaluates the items emitted by the source returning True if they pass the
        filter
        :return: filtered observable
        """

        observable = FilterObservable(self, predicate=predicate)
        return ObservableOp(observable)

    def flat_zip(self, right, selector_left, selector=None):
        observable = FlatZipObservable(left=self, right=right,
                                       selector_left=selector_left, selector=selector)
        return ObservableOp(observable)

    @classmethod
    def from_(cls, iterable: Iterable):
        """ Converts an iterable into an observable

        :param iterable:
        :return:
        """

        class ToIterableObservable(Observable):

            def unsafe_subscribe(self, observer, scheduler, subscribe_scheduler):
                iterator = iter(iterable)
                from_iterator_obs = IteratorAsObservable(iterator=iterator)
                disposable = from_iterator_obs.unsafe_subscribe(observer, scheduler, subscribe_scheduler)
                return disposable

        return ObservableOp(ToIterableObservable())

    def from_iterator(self, iterator: Iterator):
        """ Converts an iterator into an observable

        :param iterator:
        :return:
        """

        observable = IteratorAsObservable(iterator=iterator)
        return ObservableOp(observable)

    def map(self, selector: Callable[[Any], Any]):
        """ Maps each item emitted by the source by applying the given function

        :param selector: function that defines the mapping
        :return: mapped observable
        """

        observable = MapObservable(source=self, selector=selector)
        return ObservableOp(observable)

    def observe_on(self, scheduler):
        """ Operator that specifies a specific scheduler, on which observers will observe events

        :param scheduler: a rxbackpressure scheduler
        :return: an observable running on specified scheduler
        """

        observable = ObserveOnObservable(self, scheduler)
        return ObservableOp(observable)

    def pairwise(self, selector=None):
        """ Creates an observable that pairs each neighbouring two items from the source

        :param selector: (optional) selector function
        :return: paired observable
        """

        observable = PairwiseObservable(source=self, selector=selector)
        return ObservableOp(observable)

    def now(self, elem):
        """ Converts an element into an observable

        :param elem: the single item sent by the observable
        :return: single item observable
        """
        observable = NowObservable(source=self, elem=elem)
        return ObservableOp(observable)

    def repeat_first(self):
        observable = RepeatFirstObservable(source=self)
        return ObservableOp(observable)

    def replay(self):
        """ Converts this observable into a multicast observable that replays the item received by the source. Note
        that this observable is subscribed when the multicast observable is subscribed for the first time. Therefore,
        this observable is never subscribed more than once.

        :return: multicast observable
        """

        observable = ConnectableObservable(source=self, subject=ReplaySubject()).ref_count()
        return ObservableOp(observable)

    def share(self):
        """ Converts this observable into a multicast observable that backpressures only after each subscribed
        observer backpressures. Note that this observable is subscribed when the multicast observable is subscribed for
        the first time. Therefore, this observable is never subscribed more than once.

        :return: multicast observable
        """

        observable = ConnectableObservable(source=self, subject=PublishSubject()).ref_count()
        return ObservableOp(observable)

    def to_rx(self, scheduler=None):
        """ Converts this observable to an rx.Observable

        :param scheduler:
        :return:
        """

        def subscribe(observer):
            class ToRxObserver(Observer):

                def on_next(self, v):
                    observer.on_next(v)
                    return Continue()

                def on_error(self, err):
                    observer.on_error(err)

                def on_completed(self):
                    observer.on_completed()

            scheduler_ = scheduler or current_thread_scheduler
            return self.unsafe_subscribe(ToRxObserver(), scheduler_, current_thread_scheduler)

        return AnonymousObservable(subscribe)

    def window(self, right, is_lower, is_higher):
        o1, o2 = window(self, right, is_lower, is_higher)
        return ObservableOp(o1).map(lambda t2: (t2[0], ObservableOp(t2[1]))), ObservableOp(o2)

    def zip_with_index(self, selector: Callable[[Any, int], Any] = None):
        """ Zips each item emmited by the source with their indices

        :param selector: a mapping function applied over the generated pairs
        :return: zipped with index observable
        """

        observable = ZipWithIndexObservable(source=self, selector=selector)
        return ObservableOp(observable)

    def zip2(self, right, selector=None):
        """ Creates a new observable from two observables by combining their item in pairs in a strict sequence.

        :param selector: a mapping function applied over the generated pairs
        :return: zipped observable
        """

        observable = Zip2Observable(left=self, right=right, selector=selector)
        return ObservableOp(observable)
