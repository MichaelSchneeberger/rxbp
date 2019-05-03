import threading
from queue import Queue
from typing import Iterable

import rx

from rxbp.ack import Ack, Continue, Stop
from rxbp.observables.iteratorasobservable import IteratorAsObservable
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler


class ConnectableSubscriber(Observer):
    def __init__(self, underlying: Observer, scheduler: Scheduler, subscribe_scheduler: Scheduler):
        self.underlying = underlying
        self.scheduler = scheduler
        self.subscribe_scheduler = subscribe_scheduler

        self.root_ack = Ack()
        self.connected_ack = self.root_ack
        self.connected_ref = None
        self.is_connected = False
        self.is_connected_started = False
        self.scheduled_done = False
        self.schedule_error = None
        self.was_canceled = False
        self.queue = Queue()
        self.lock = threading.RLock()

    def connect(self):
        with self.lock:
            if not self.is_connected and not self.is_connected_started:
                self.is_connected_started = True

                buffer_was_drained = Ack()

                def on_next(v):
                    if isinstance(v, Continue):
                        self.root_ack.on_next(Continue())
                        self.root_ack.on_completed()
                        source.is_connected = True
                        source.queue = None
                        # source.connected_ack = None
                        # todo: fill in
                    elif isinstance(v, Stop):
                        raise NotImplementedError
                    else:
                        raise Exception('illegal acknowledgment value {}'.format(v))

                buffer_was_drained.subscribe(on_next=on_next)

                source = self

                class CustomObserver(Observer):
                    def __init__(self):
                        self.ack = None

                    def on_next(self, v):
                        ack = source.underlying.on_next(v)
                        self.ack = ack

                        def on_next(v):
                            if isinstance(v, Stop):
                                buffer_was_drained.on_next(v)
                                buffer_was_drained.on_completed()

                        ack.subscribe(on_next=on_next)

                        return ack

                    def on_error(self, err):
                        raise NotImplementedError

                    def on_completed(self):
                        if not source.scheduled_done:
                            if self.ack is None:
                                buffer_was_drained.on_next(Continue())
                                buffer_was_drained.on_completed()
                            else:
                                def on_next(v):
                                    if isinstance(v, Continue):
                                        buffer_was_drained.on_next(Continue())
                                        buffer_was_drained.on_completed()

                                self.ack.subscribe(on_next)
                        elif source.schedule_error is not None:
                            raise NotImplementedError
                        else:
                            source.underlying.on_completed()

                class EmptyObject:
                    pass

                self.queue.put(EmptyObject)
                disposable = IteratorAsObservable(iter(self.queue.get, EmptyObject), scheduler=self.scheduler,
                                                  subscribe_scheduler=self.subscribe_scheduler) \
                    .observe(CustomObserver())

                self.connected_ref = buffer_was_drained, disposable
        return self.connected_ref

    def push_first(self, elem):
        with self.lock:
            if self.is_connected or self.is_connected_started:
                throw_exception = True
            elif not self.scheduled_done:
                throw_exception = False
                self.queue.put(elem, block=False)
            else:
                throw_exception = False

        if throw_exception:
            raise Exception('Observer was already connected, so cannot pushFirst')

    def push_first_all(self, cs: Iterable):
        with self.lock:
            if self.is_connected or self.is_connected_started:
                throw_exception = True
            elif not self.scheduled_done:
                throw_exception = False
                for elem in cs:
                    self.queue.put(elem, block=False)
            else:
                throw_exception = False

        if throw_exception:
            raise Exception('Observer was already connected, so cannot pushFirst')

    def push_complete(self):
        with self.lock:
            if self.is_connected or self.is_connected_started:
                throw_exception = True
            elif not self.scheduled_done:
                throw_exception = False
                self.scheduled_done = True
            else:
                throw_exception = False

        if throw_exception:
            raise Exception('Observer was already connected, so cannot pushFirst')

    def push_error(self, ex: Exception):
        with self.lock:
            if self.is_connected or self.is_connected_started:
                throw_exception = True
            elif not self.scheduled_done:
                throw_exception = False
                self.scheduled_done = True
                self.schedule_error = ex
            else:
                throw_exception = False

        if throw_exception:
            raise Exception('Observer was already connected, so cannot pushFirst')

    def on_next(self, elem):
        if not self.is_connected:
            def __(v):
                if isinstance(v, Continue):
                    ack = self.underlying.on_next(elem)
                    if isinstance(ack, Continue):
                        return rx.just(ack)
                    elif isinstance(ack, Stop):
                        raise NotImplementedError
                    else:
                        return ack
                else:
                    return Stop()

            new_ack = Ack()
            self.connected_ack.pipe(
                rx.operators.observe_on(self.scheduler),
                rx.operators.flat_map(__),
            ).subscribe(new_ack)
            self.connected_ack = new_ack
            return self.connected_ack
        elif not self.was_canceled:
            ack = self.underlying.on_next(elem)
            return ack
        else:
            return Stop()

    def on_error(self, err):
        def on_next(v):
            if isinstance(v, Continue):
                self.underlying.on_error(err)
        self.connected_ack.pipe(rx.operators.observe_on(self.scheduler)) \
            .subscribe(on_next=on_next)

    def on_completed(self):
        def on_next(v):
            if isinstance(v, Continue):
                self.underlying.on_completed()
        self.connected_ack.pipe(rx.operators.observe_on(self.scheduler)) \
            .subscribe(on_next=on_next)
