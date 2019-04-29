import threading
from typing import Iterable, Set, List


from rx.disposable import Disposable

from rxbp.ack import Continue, Stop
from rxbp.observers.connectablesubscriber import ConnectableSubscriber
from rxbp.observables.iteratorasobservable import IteratorAsObservable
from rxbp.observer import Observer
from rxbp.internal.promisecounter import PromiseCounter
from rxbp.scheduler import SchedulerBase
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.subjects.subjectbase import SubjectBase


class ReplaySubject(SubjectBase):

    class State:
        def __init__(self,
                     buffer: List,
                     capacity: int,
                     subscribers: Set = set(),
                     length : int = 0,
                     is_done: bool = False,
                     error_thrown: Exception = None):
            self.buffer = buffer
            self.capacity = capacity
            self.subscribers = subscribers
            self.length = length
            self.is_done = is_done
            self.error_thrown = error_thrown

        def copy(self, buffer=None, length=None, subscribers=None):
            return ReplaySubject.State(buffer=buffer if buffer is not None else self.buffer,
                                       capacity=self.capacity,
                                       subscribers=subscribers if subscribers is not None else self.subscribers,
                                       length=length if length is not None else self.length,
                                       is_done=self.is_done,
                                       error_thrown=self.error_thrown)

        def append_elem(self, elem) -> 'ReplaySubject.State':
            if self.capacity == 0:
                return self.copy(buffer = self.buffer + [elem])
            elif self.length >= self.capacity:
                raise NotImplementedError
            else:
                return self.copy(buffer=self.buffer + [elem], length=self.length+1)

        def add_new_subscriber(self, s):
            subscribers = self.subscribers.copy()
            subscribers = subscribers | {s}

            new_state = self.copy(subscribers=subscribers)
            return new_state

        def remove_subscriber(self, to_remove):
            subscribers = self.subscribers.copy()
            if to_remove in subscribers:  # todo: remove this
                subscribers.remove(to_remove)

            return self.copy(subscribers=subscribers)

        def mark_done(self, ex: Exception):
            return ReplaySubject.State(buffer=self.buffer, capacity=self.capacity, subscribers=set(),
                                length=self.length, is_done=True, error_thrown=ex)

    def __init__(self, initial_state: State = None):
        self.state: ReplaySubject.State = initial_state or ReplaySubject.State(buffer=[], capacity=0)

        self.lock = threading.RLock()
        # self.batch_size = batch_size

    def unsafe_subscribe(self, observer: Observer, scheduler: SchedulerBase, subscribe_scheduler: SchedulerBase):
        """ Creates a new ConnectableSubscriber for each subscription, pushes the current buffer to the
        ConnectableSubscriber and connects it immediately

        """

        def stream_on_done(buffer: Iterable, error_thrown: Exception = None) -> Disposable:
            class TObserver(Observer):

                def on_next(self, v):
                    ack = observer.on_next(v)
                    return ack

                def on_error(self, err):
                    observer.on_error(err)

                def on_completed(self):
                    if error_thrown is not None:
                        observer.on_error(error_thrown)
                    else:
                        observer.on_completed()


            return IteratorAsObservable(iter(buffer)) \
                .subscribe_observer(TObserver(), scheduler, TrampolineScheduler())

        state = self.state
        buffer = state.buffer

        if state.is_done:
            return stream_on_done(buffer, state.error_thrown)
        else:
            c = ConnectableSubscriber(observer, scheduler=scheduler)
            with self.lock:
                new_state = self.state.add_new_subscriber(c)
                self.state = new_state

            c.push_first_all(buffer)
            ack, disposable = c.connect()

            if isinstance(ack, Stop):
                self.remove_subscriber(c)
            elif not isinstance(ack, Continue):
                def on_next(v):
                    if isinstance(v, Stop):
                        self.remove_subscriber(c)
                ack.subscribe(on_next=on_next)

            def _():
                try:
                    self.remove_subscriber(c)
                finally:
                    disposable.dispose()
            return Disposable(_)

    def on_complete_or_error(self, ex: Exception = None):
        with self.lock:
            state = self.state

            if not state.is_done:
                self.state = state.mark_done(ex)

        iterator = iter(state.subscribers)
        for obs in iterator:
            if ex is None:
                obs.on_completed()
            else:
                obs.on_error(ex)

    def remove_subscriber(self, s: ConnectableSubscriber):
        with self.lock:
            # print('remove subscriber')
            state = self.state
            new_state = state.remove_subscriber(s)
            self.state = new_state

    def on_next(self, elem):
        with self.lock:
            state = self.state
            if not state.is_done:
                self.state = state.append_elem(elem)

        iterator = iter(state.subscribers)
        result = None

        for obs in iterator:
            try:
                ack = obs.on_next(elem)
            except:
                raise NotImplementedError

            if isinstance(ack, Stop):
                self.remove_subscriber(obs)
            else:
                if result is None:
                    result = PromiseCounter(Continue(), 1)
                result.acquire()

                def on_next(v):
                    if isinstance(v, Continue):
                        result.countdown()
                    else:
                        self.remove_subscriber(obs)
                        result.countdown()

                ack.observe_on(obs.scheduler).subscribe(on_next)

        if result is None:
            return Continue()
        else:
            result.countdown()
            return result.promise

    def on_error(self, err):
        self.on_complete_or_error(err)

    def on_completed(self):
        self.on_complete_or_error()
