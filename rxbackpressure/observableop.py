from rx import AnonymousObservable
from rx.concurrency.schedulerbase import SchedulerBase
from rx.disposables import CompositeDisposable

from rxbackpressure.schedulers.currentthreadscheduler import current_thread_scheduler
from rxbackpressure.subjects.cachedservefirstsubject import CachedServeFirstSubject
from rxbackpressure.observables.concatmapobservable import ConcatMapObservable
from rxbackpressure.observables.connectableobservable import ConnectableObservable
from rxbackpressure.observables.flatzipobservable import FlatZipObservable
from rxbackpressure.testing.debugobservable import DebugObservable
from rxbackpressure.observables.filterobservable import FilterObservable
from rxbackpressure.observables.iteratorasobservable import IteratorAsObservable
from rxbackpressure.observables.nowobservable import NowObservable
from rxbackpressure.scheduler import SchedulerBase
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

    def concat_map(self, selector):
        observable = ConcatMapObservable(source=self,
                                         selector=selector)
        return ObservableOp(observable)

    @classmethod
    def from_(cls, iterator):
        observable = IteratorAsObservable(iterator=iterator)
        return ObservableOp(observable)

    def pairwise(self, selector=None):
        observable = PairwiseObservable(source=self, selector=selector)
        return ObservableOp(observable)

    def map(self, selector):
        observable = MapObservable(source=self, selector=selector)
        return ObservableOp(observable)

    def zip_count(self, selector=None):
        observable = ZipWithIndexObservable(source=self, selector=selector)
        return ObservableOp(observable)

    def pure(self, elem):
        observable = NowObservable(source=self, elem=elem)
        return ObservableOp(observable)

    def share(self):
        observable = ConnectableObservable(source=self).ref_count()
        return ObservableOp(observable)

    def window(self, right, is_lower, is_higher):
        o1, o2 = window(self, right, is_lower, is_higher)
        return ObservableOp(o1).map(lambda t2: (t2[0], ObservableOp(t2[1]))), ObservableOp(o2)

    def cache(self):
        source = self

        class CacheObservable(Observable):
            def unsafe_subscribe(self, observer: Observer, scheduler: SchedulerBase, subscribe_scheduler: SchedulerBase):
                subject = CachedServeFirstSubject()
                d2 = subject.subscribe(observer, scheduler, subscribe_scheduler)
                d1 = source.unsafe_subscribe(subject, scheduler, subscribe_scheduler)
                return CompositeDisposable(d1, d2)

        return ObservableOp(CacheObservable())

    def to_rx(self, scheduler=None):
        def subscribe(observer):
            scheduler_ = scheduler or current_thread_scheduler
            return self.unsafe_subscribe(observer, scheduler_, current_thread_scheduler)

        return AnonymousObservable(subscribe)

    def zip2(self, right, selector=None):
        observable = Zip2Observable(left=self, right=right, selector=selector)
        return ObservableOp(observable)

    def flat_zip(self, right, selector_left, selector=None):
        observable = FlatZipObservable(left=self, right=right,
                                       selector_left=selector_left, selector=selector)
        return ObservableOp(observable)

    def observe_on(self, scheduler):
        observable = ObserveOnObservable(self, scheduler)
        return ObservableOp(observable)

    def debug(self, name, on_next=None, on_subscribe=None, on_ack=None, print_ack=None):
        observable = DebugObservable(self, name=name, on_next=on_next, on_subscribe=on_subscribe, on_ack=on_ack,
                                     print_ack=print_ack)
        return ObservableOp(observable)

    def filter(self, predicate):
        observable = FilterObservable(self, predicate=predicate)
        return ObservableOp(observable)
