import itertools
from typing import Callable, Any, Iterator, Iterable, List

from rx import AnonymousObservable

from rxbackpressurebatched.ack import Continue
from rxbackpressurebatched.observables.flatmapobservablemulti import FlatMapObservableMulti
from rxbackpressurebatched.observables.flatzipobservablemulti import FlatZipObservableMulti
from rxbackpressurebatched.observables.repeatfirstobservable import RepeatFirstObservable
from rxbackpressurebatched.observables.controlledzipobservable import ControlledZipObservable
from rxbackpressurebatched.observables.windowmulti import window_multi
from rxbackpressurebatched.schedulers.currentthreadscheduler import current_thread_scheduler
from rxbackpressurebatched.subjects.cachedservefirstsubject import CachedServeFirstSubject
from rxbackpressurebatched.observables.flatmapobservable import FlatMapObservable
from rxbackpressurebatched.observables.connectableobservable import ConnectableObservable
from rxbackpressurebatched.observables.flatzipobservable import FlatZipObservable
from rxbackpressurebatched.subjects.publishsubject import PublishSubject
from rxbackpressurebatched.subjects.replaysubject import ReplaySubject
from rxbackpressurebatched.testing.debugobservable import DebugObservable
from rxbackpressurebatched.observables.filterobservable import FilterObservable
from rxbackpressurebatched.observables.iteratorasobservable import IteratorAsObservable
from rxbackpressurebatched.observables.nowobservable import NowObservable
from rxbackpressurebatched.scheduler import SchedulerBase, Scheduler
from rxbackpressurebatched.observables.window import window
from rxbackpressurebatched.observables.zipwithindexobservable import ZipWithIndexObservable
from rxbackpressurebatched.observables.mapobservable import MapObservable
from rxbackpressurebatched.observable import Observable
from rxbackpressurebatched.observables.observeonobservable import ObserveOnObservable
from rxbackpressurebatched.observer import Observer
from rxbackpressurebatched.observables.pairwiseobservable import PairwiseObservable
from rxbackpressurebatched.observables.zip2observable import Zip2Observable


class ObservableOpMulti(Observable):
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
    #     return ObservableOpMulti(ToBackpressureObservable())

    def cache(self):
        """ Converts this observable into a multicast observable that caches the items that the fastest observer has
        already received and the slowest observer has not yet requested. Note that this observable is subscribed when
        the multicast observable is subscribed for the first time. Therefore, this observable is never subscribed more
        than once.

        :return: multicast observable
        """

        observable = ConnectableObservable(source=self, subject=CachedServeFirstSubject()).ref_count()
        return ObservableOpMulti(observable)

    def flat_map(self, selector):
        """ Applies a function to each item emitted by the source and flattens the result. The function takes any type
        as input and returns an inner observable. The resulting observable concatenates the items of each inner
        observable.

        :param selector: A function that takes any type as input and returns an observable.
        :return: a flattened observable
        """

        observable = FlatMapObservableMulti(source=self,
                                       selector=selector)
        return ObservableOpMulti(observable)

    def debug(self, name=None, on_next=None, on_subscribe=None, on_ack=None, print_ack=None, on_ack_msg=None):
        observable = DebugObservable(self, name=name, on_next=on_next, on_subscribe=on_subscribe, on_ack=on_ack,
                                     print_ack=print_ack, on_ack_msg=on_ack_msg)
        return ObservableOpMulti(observable)

    def execute_on(self, scheduler: Scheduler):
        source = self

        class ExecuteOnObservable(Observable):

            def unsafe_subscribe(self, observer, _, subscribe_scheduler):
                disposable = source.unsafe_subscribe(observer, scheduler, subscribe_scheduler)
                return disposable

        return ObservableOpMulti(ExecuteOnObservable())

    def filter(self, predicate: Callable[[Any], bool]):
        """ Only emits those items for which the given predicate holds

        :param predicate: a function that evaluates the items emitted by the source returning True if they pass the
        filter
        :return: filtered observable
        """

        observable = FilterObservable(self, predicate=predicate)
        return ObservableOpMulti(observable)

    def flat_zip(self, right, selector_inner, selector_left=None, selector=None):
        observable = FlatZipObservableMulti(left=self, right=right,
                                       selector_inner=selector_inner, selector_left=selector_left,
                                       selector=selector)
        return ObservableOpMulti(observable)

    @classmethod
    def from_(cls, iterable: Iterable, batch_size=1):
        """ Converts an iterable into an observable

        :param iterable:
        :return:
        """

        class ToIterableObservable(Observable):

            def unsafe_subscribe(self, observer, scheduler, subscribe_scheduler):
                iterator = iter(iterable)
                from_iterator_obs = cls.from_iterator(iterator=iterator, batch_size=batch_size)
                disposable = from_iterator_obs.unsafe_subscribe(observer, scheduler, subscribe_scheduler)
                return disposable

        return ObservableOpMulti(ToIterableObservable())

    @classmethod
    def from_iterator(cls, iterator: Iterator, batch_size=1):
        """ Converts an iterator into an observable

        :param iterator:
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
        return ObservableOpMulti(observable)

    @classmethod
    def from_list(cls, buffer: List, batch_size: int):

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

        return ObservableOpMulti(ToIterableObservable())

    def map(self, selector: Callable[[Any], Any]):
        """ Maps each item emitted by the source by applying the given function

        :param selector: function that defines the mapping
        :return: mapped observable
        """

        observable = MapObservable(source=self, selector=selector)
        return ObservableOpMulti(observable)

    def map_count(self, selector: Callable[[Any, int], Any] = None):
        """ Zips each item emmited by the source with their indices

        :param selector: a mapping function applied over the generated pairs
        :return: zipped with index observable
        """

        observable = ZipWithIndexObservable(source=self, selector=selector)
        return ObservableOpMulti(observable)

    def observe_on(self, scheduler):
        """ Operator that specifies a specific scheduler, on which observers will observe events

        :param scheduler: a rxbackpressure scheduler
        :return: an observable running on specified scheduler
        """

        observable = ObserveOnObservable(self, scheduler)
        return ObservableOpMulti(observable)

    def pairwise(self):
        """ Creates an observable that pairs each neighbouring two items from the source

        :param selector: (optional) selector function
        :return: paired observable
        """

        observable = PairwiseObservable(source=self)
        return ObservableOpMulti(observable)

    @classmethod
    def now(cls, elem):
        """ Converts an element into an observable

        :param elem: the single item sent by the observable
        :return: single item observable
        """
        observable = NowObservable(elem=elem)
        return ObservableOpMulti(observable)

    def repeat_first(self):
        """ Repeat the first item forever

        :return:
        """
        observable = RepeatFirstObservable(source=self)
        return ObservableOpMulti(observable)

    def replay(self):
        """ Converts this observable into a multicast observable that replays the item received by the source. Note
        that this observable is subscribed when the multicast observable is subscribed for the first time. Therefore,
        this observable is never subscribed more than once.

        :return: multicast observable
        """

        observable = ConnectableObservable(source=self, subject=ReplaySubject()).ref_count()
        return ObservableOpMulti(observable)

    def share(self):
        """ Converts this observable into a multicast observable that backpressures only after each subscribed
        observer backpressures. Note that this observable is subscribed when the multicast observable is subscribed for
        the first time. Therefore, this observable is never subscribed more than once.

        :return: multicast observable
        """

        observable = ConnectableObservable(source=self, subject=PublishSubject()).ref_count()
        return ObservableOpMulti(observable)

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

    def window(self, right: Observable, is_lower, is_higher):
        """ Forward each item from the left Observable by attaching an inner Observable to it. Subdivide or reject
        items from the right Observable via is_lower and is_higher functions, and emit each item of a subdivision (or window)
        in the inner Observable

        :param right:
        :param is_lower:
        :param is_higher:
        :return:
        """

        o1, o2 = window_multi(self, right, is_lower, is_higher)
        # return ObservableOpMulti(o1).map(lambda t2: (t2[0], ObservableOpMulti(t2[1]))), ObservableOpMulti(o2)
        return ObservableOpMulti(o1), ObservableOpMulti(o2)

    def controlled_zip(self, right, is_lower, is_higher, selector):
        observable = ControlledZipObservable(left=self, right=right, is_lower=is_lower, is_higher=is_higher,
                                             selector=selector)
        return ObservableOpMulti(observable)

    def zip(self, right, selector=None):
        """ Creates a new observable from two observables by combining their item in pairs in a strict sequence.

        :param selector: a mapping function applied over the generated pairs
        :return: zipped observable
        """

        observable = Zip2Observable(left=self, right=right, selector=selector)
        return ObservableOpMulti(observable)
