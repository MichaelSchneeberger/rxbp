import threading
from typing import Iterable, Set, List

from rx.disposable import Disposable
from rxbp.ack.ackimpl import Continue, Stop
from rxbp.ack.observeon import _observe_on
from rxbp.ack.single import Single

from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.observables.iteratorasobservable import IteratorAsObservable
from rxbp.observer import Observer
from rxbp.internal.promisecounter import PromiseCounter
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import SchedulerBase, Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.observablesubjects.osubjectbase import OSubjectBase


class ReplayOSubject(OSubjectBase):

    class State:
        def __init__(self,
                     buffer: List,
                     capacity: int,
                     subscribers: Set = set(),
                     length : int = 0,
                     is_done: bool = False,
                     error_thrown: Exception = None,
                     ):
            self.buffer = buffer
            self.capacity = capacity
            self.subscribers = subscribers
            self.length = length
            self.is_done = is_done
            self.error_thrown = error_thrown

        def copy(self, buffer=None, length=None, subscribers=None):
            return ReplayOSubject.State(buffer=buffer if buffer is not None else self.buffer,
                                        capacity=self.capacity,
                                        subscribers=subscribers if subscribers is not None else self.subscribers,
                                        length=length if length is not None else self.length,
                                        is_done=self.is_done,
                                        error_thrown=self.error_thrown)

        def append_elem(self, elem) -> 'ReplayOSubject.State':
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
            return ReplayOSubject.State(buffer=self.buffer, capacity=self.capacity, subscribers=set(),
                                        length=self.length, is_done=True, error_thrown=ex)

    def __init__(self, scheduler: SchedulerBase, subscribe_scheduler: Scheduler, initial_state: State = None):
        self.state: ReplayOSubject.State = initial_state or ReplayOSubject.State(buffer=[], capacity=0)

        self.scheduler = scheduler
        self.subscribe_scheduler = subscribe_scheduler

        self.lock = threading.RLock()
        # self.batch_size = batch_size

    def observe(self, observer_info: ObserverInfo):
        """ Creates a new ConnectableSubscriber for each subscription, pushes the current buffer to the
        ConnectableSubscriber and connects it immediately

        """
        observer = observer_info.observer

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

            t_subscription = observer_info.copy(TObserver())
            return IteratorAsObservable(iter(buffer), scheduler=self.scheduler, subscribe_scheduler=TrampolineScheduler()) \
                .observe(t_subscription)

        state = self.state
        buffer = state.buffer

        if state.is_done:
            return stream_on_done(buffer, state.error_thrown)
        else:
            c = ConnectableObserver(observer, scheduler=self.scheduler, subscribe_scheduler=self.subscribe_scheduler)
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

    def remove_subscriber(self, s: ConnectableObserver):
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

                class InnerSingle(Single):
                    def on_error(self, exc: Exception):
                        raise NotImplementedError

                    def on_next(_, v):
                        if isinstance(v, Continue):
                            result.countdown()
                        else:
                            self.remove_subscriber(obs)
                            result.countdown()

                _observe_on(ack, obs.scheduler).subscribe(InnerSingle())
                # ack.pipe(rx.operators.observe_on(obs.scheduler)).subscribe(on_next)

        if result is None:
            return Continue()
        else:
            result.countdown()
            return result.promise

    def on_error(self, err):
        self.on_complete_or_error(err)

    def on_completed(self):
        self.on_complete_or_error()
