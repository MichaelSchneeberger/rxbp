import threading
from typing import Set, Tuple, List, Union

import rx
from rx.disposable import Disposable

from rxbp.ack import Continue, stop_ack, continue_ack
from rxbp.observer import Observer
from rxbp.internal.promisecounter import PromiseCounter
from rxbp.scheduler import Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.subjects.subjectbase import SubjectBase


class PublishSubject(SubjectBase):
    def __init__(self, scheduler: Scheduler):

        super().__init__()

        self.state = self.State()
        self.lock = threading.RLock()
        self.scheduler = scheduler

    class Subscriber:
        def __init__(self, observer, scheduler):
            self.observer = observer
            self.scheduler = scheduler

    class Empty:
        """ if state.subscriber is Empty, then the subject has completed
        """

        pass

    class State:
        def __init__(self, subscribers: Union[Set['PublishSubject.Subscriber'], 'PublishSubject.Empty'] = None,
                     cache: List = None, error_thrown=None):
            self.subscribers = subscribers or set()
            self.cache = cache
            self.error_thrown = error_thrown

        def refresh(self):
            """ Probably it also works without the cache

            :return:
            """
            return PublishSubject.State(cache=list(self.subscribers))

        def is_done(self):
            return isinstance(self.subscribers, PublishSubject.Empty)

        def complete(self, error_thrown):
            if isinstance(self.subscribers, PublishSubject.Empty):
                return self
            else:
                return PublishSubject.State(error_thrown=error_thrown,
                                            subscribers=PublishSubject.Empty(),
                                            cache=None)

    def on_subscribe_completed(self, subscriber: Subscriber, ex):
        if ex is not None:
            subscriber.observer.on_error(ex)
        else:
            subscriber.observer.on_completed()
        return Disposable()

    def observe(self, observer: Observer):
        state = self.state
        subscribers = state.subscribers

        subscriber = self.Subscriber(observer, scheduler=TrampolineScheduler())  # todo: remove scheduler
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
                disposable = Disposable(dispose)
                return disposable
            else:
                return self.observe(observer)

    def on_next(self, elem):
        state = self.state
        subscribers = state.cache

        if subscribers is None:
            sub_set = state.subscribers
            if isinstance(sub_set, self.Empty):
                return stop_ack
            else:
                update = state.refresh()
                self.state = update
                return self.send_on_next_to_all(update.cache, elem)
        else:
            return self.send_on_next_to_all(subscribers, elem)

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

                    ack.pipe(rx.operators.first()).subscribe(on_next=on_next, on_error=on_error)

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

    def unsubscribe(self, subscriber):
        state = self.state
        subscribers = state.subscribers

        if state.cache is None:
            return continue_ack
        else:
            update = self.State(subscribers = subscribers - {subscriber})
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
