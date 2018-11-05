from rx import config
from rx.concurrency.schedulerbase import SchedulerBase

from rxbackpressure.ack import Continue
from rxbackpressure.observable import Observable
from rxbackpressure.observer import Observer
from rxbackpressure.internal.promisecounter import PromiseCounter


class PublishSubject(Observable, Observer):
    def __init__(self):
        self.lock = config["concurrency"].RLock()
        self.subscribers = []
        self.is_freezed = False

    def unsafe_subscribe(self, observer: Observer, scheduler: SchedulerBase,
                         subscribe_scheduler: SchedulerBase):
        assert self.is_freezed == False, 'no subscriptions allows after freeze'

        # todo: is lock required?
        with self.lock:
            if self.subscribers is None:
                raise NotImplementedError
            else:
                self.subscribers.append((observer, subscribe_scheduler))

    def on_next(self, v):
        subscribers = []
        with self.lock:
            if self.subscribers is None:
                send_to_all = False
                raise NotImplementedError
            else:
                send_to_all = True
                subscribers = self.subscribers

        if send_to_all:
            return self.send_on_next_to_all(subscribers, v)
        else:
            raise NotImplementedError

    def on_error(self, exc):
        self.send_oncomplete_or_error(exc)

    def on_completed(self):
        self.send_oncomplete_or_error()

    def send_on_next_to_all(self, subscribers, v):
        result = None

        index = 0
        while index < len(self.subscribers):
            observer, _ = subscribers[index]
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
        subscribers = self.subscribers

        if exc is None:
            for observer, _ in subscribers:
                observer.on_completed()
        else:
            for observer, _ in subscribers:
                observer.on_error(exc)

    def unsubscribe(self, subscriber):
        subscribers = self.subscribers

        if len(subscribers) == 0:
            return Continue
        else:
            with self.lock:
                self.subscribers = [s for s in self.subscribers if s is not subscriber]
            return Continue
