from typing import Set, Tuple, List, Union

from rx import config
from rx.concurrency.schedulerbase import SchedulerBase
from rx.core import Disposable

from rxbackpressure.ack import Continue, stop_ack, continue_ack
from rxbackpressure.observable import Observable
from rxbackpressure.observer import Observer
from rxbackpressure.internal.promisecounter import PromiseCounter


class PublishSubject(Observable, Observer):
    def __init__(self):
        # self.subscribers = []
        # self.is_freezed = False

        self.state = self.State()
        self.lock = config["concurrency"].RLock()

    class Subscriber:
        def __init__(self, observer, scheduler):
            self.observer = observer
            self.scheduler = scheduler

    class Empty:
        pass

    class State:
        def __init__(self, subscribers: Union[Set['PublishSubject.Subscriber'], 'PublishSubject.Empty'] = None,
                     cache: List = None, error_thrown = None):
            self.subscribers = subscribers or set()
            self.cache = cache
            self.error_thrown = error_thrown

        def refresh(self):
            return PublishSubject.State(cache=list(self.subscribers))

        def is_done(self):
            return isinstance(self.subscribers, PublishSubject.Empty)

        def complete(self, error_thrown):
            if isinstance(self.subscribers, PublishSubject.Empty):
                return self
            else:
                return PublishSubject.State(error_thrown=error_thrown)

    def on_subscribe_completed(self, subscriber, ex):
        if ex is not None:
            subscriber.on_error(ex)
        else:
            subscriber.on_completed()
        return Disposable.empty()

    def unsafe_subscribe(self, observer: Observer, scheduler: SchedulerBase,
                         subscribe_scheduler: SchedulerBase):
        state = self.state
        subscribers = state.subscribers

        subscriber = self.Subscriber(observer, subscribe_scheduler)
        if isinstance(subscribers, self.Empty):
            self.on_subscribe_completed(subscriber, state.error_thrown)
        else:
            update_set = subscribers | {subscriber}
            update = self.State(subscribers=update_set)

            with self.lock:
                if self.state is state:
                    is_updated = True
                    self.state = update
                else:
                    is_updated = False

            if is_updated:
                def dispose():
                    self.unsubscribe(subscriber)
                return Disposable.create(dispose)
            else:
                return self.unsafe_subscribe(observer, scheduler, subscribe_scheduler)

        # assert self.is_freezed == False, 'no subscriptions allows after freeze'
        #
        # # todo: is lock required?
        # with self.lock:
        #     if self.subscribers is None:
        #         raise NotImplementedError
        #     else:
        #         self.subscribers.append((observer, subscribe_scheduler))

    def on_next(self, elem):
        state = self.state
        subscribers = state.cache

        if subscribers is None:
            sub_set = state.subscribers
            if sub_set is None:
                return stop_ack
            else:
                update = state.refresh()
                self.state = update
                return self.send_on_next_to_all(update.cache, elem)
        else:
            return self.send_on_next_to_all(subscribers, elem)

        # subscribers = []
        # with self.lock:
        #     if self.subscribers is None:
        #         send_to_all = False
        #         raise NotImplementedError
        #     else:
        #         send_to_all = True
        #         subscribers = self.subscribers
        #
        # if send_to_all:
        #     return self.send_on_next_to_all(subscribers, v)
        # else:
        #     raise NotImplementedError

    def on_error(self, exc):
        self.send_oncomplete_or_error(exc)

    def on_completed(self):
        self.send_oncomplete_or_error()

    def send_on_next_to_all(self, subscribers: List, v):
        result = None

        index = 0
        while index < len(subscribers):
            subscriber = subscribers[index]
            observer = subscriber.observer
            index += 1

            try:
                ack = observer.on_next(v)
            except:
                raise NotImplementedError

            if ack.has_value:
                if not isinstance(ack, Continue) and ack.exception is not None:
                    self.unsubscribe(observer)
            else:

                if not isinstance(ack, Continue):
                    if result is None:
                        result = PromiseCounter(Continue(), 1)

                    result.acquire()

                    def on_next(v):
                        if isinstance(v, Continue):
                            result.countdown()
                        else:
                            self.unsubscribe(observer)
                            result.countdown()

                    def on_error(err):
                        self.unsubscribe(observer)
                        result.countdown()

                    ack.first().subscribe(on_next=on_next, on_error=on_error)

        if result is None:
            return Continue()
        else:
            result.countdown()
            return result.promise

    def send_oncomplete_or_error(self, exc: Exception = None):
        state = self.state
        sub_set = state.subscribers

        if state.cache is not None:
            subscribers = set(state.cache)
        else:
            subscribers = sub_set

        if not isinstance(subscribers, self.Empty):
            update = state.complete(exc)
            with self.lock:
                if self.state is state:
                    is_updated = True
                    self.state = update
                else:
                    is_updated = False

            if is_updated:
                for ref in subscribers:
                    if exc is not None:
                        ref.observer.on_error(exc)
                    else:
                        ref.observer.on_completed()
            else:
                self.send_oncomplete_or_error(exc)

        # subscribers = self.subscribers
        #
        # if exc is None:
        #     for observer, _ in subscribers:
        #         observer.on_completed()
        # else:
        #     for observer, _ in subscribers:
        #         observer.on_error(exc)

    def unsubscribe(self, subscriber):
        state = self.state
        subscribers = state.subscribers

        if state.cache is None:
            return continue_ack
        else:
            update = self.State(subscribers = subscribers - subscriber)
            with self.lock:
                if self.state is state:
                    is_updated = True
                    self.state = update
                else:
                    is_updated = False

            if is_updated:
                return continue_ack
            else:
                return self.unsubscribe(subscriber)

        # subscribers = self.subscribers
        #
        # if len(subscribers) == 0:
        #     return Continue
        # else:
        #     with self.lock:
        #         self.subscribers = [s for s in self.subscribers if s is not subscriber]
        #     return Continue
