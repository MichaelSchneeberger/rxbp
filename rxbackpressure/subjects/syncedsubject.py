import traceback

from rx import config
from rx.concurrency import current_thread_scheduler
from rx.core import Disposable
from rx.disposables import CompositeDisposable, SingleAssignmentDisposable
from rx.internal import DisposedException

from rxbackpressure.backpressuretypes.syncedbackpressure import SyncedBackpressure
from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.core.backpressureobserver import BackpressureObserver


class InnerSubscription(SingleAssignmentDisposable):
    def __init__(self, observer, remove_func):
        super().__init__()

        self.remove_func = remove_func
        self.observer = observer

    def dispose(self):
        # with self.lock:
        #     if not self.subject.is_disposed and self.observer:
        #         if self.observer in self.subject.observers:
        #             self.subject.observers.remove(self.observer)
        #         self.observer = None

        self.remove_func(self.observer)

        super().dispose()


class SyncedSubject(BackpressureObservable, BackpressureObserver):
    def __init__(self, scheduler=None):
        super().__init__()

        self.is_disposed = False
        self.is_stopped = False
        self.observers = []
        self.schedulers = []
        self.inner_subscripitons = {}
        self.exception = None
        # self.scheduler = scheduler or current_thread_scheduler

        self.lock = config["concurrency"].RLock()
        self.backpressure = None

    def check_disposed(self):
        if self.is_disposed:
            raise DisposedException()

    def subscribe_backpressure(self, backpressure, scheduler=None):
        self.backpressure = SyncedBackpressure(backpressure=backpressure, scheduler=scheduler)

        for inner_subscription, scheduler in zip(self.inner_subscripitons.values(), self.schedulers):

            # def action(_, __, inner_subscription=inner_subscription):
            inner_subscription.disposable = self.backpressure.add_observer(inner_subscription.observer, scheduler)

            # try:
            # current_thread_scheduler.schedule(action)
            # except:
            #     traceback.print_exc()

        return Disposable.empty()

    def _subscribe_core(self, observer, scheduler):
        # print('subscribe synced subject')
        with self.lock:
            self.check_disposed()
            if not self.is_stopped:

                def remove_observer(observer):
                    with self.lock:
                        if not self.is_disposed and observer in self.inner_subscripitons:
                            del self.inner_subscripitons[observer]
                            if observer in self.observers:
                                self.observers.remove(observer)

                inner_subscription = InnerSubscription(remove_func=remove_observer, observer=observer)
                self.inner_subscripitons[observer] = inner_subscription

                self.observers.append(observer)
                self.schedulers.append(scheduler)
                # disposable = SingleAssignmentDisposable()

                # def action(_, __):
                #     disposable.disposable = self.backpressure.add_observer(observer)
                #
                # scheduler.schedule(action)
                # return CompositeDisposable(InnerSubscription(self, observer), disposable)
                return inner_subscription

            if self.exception:
                observer.on_error(self.exception)
                return Disposable.empty()

            observer.on_completed()
            return Disposable.empty()

    def on_completed(self):
        self._on_completed_core()

    def _on_completed_core(self):
        """Notifies all subscribed observers of the end of the sequence."""

        os = None
        with self.lock:
            self.check_disposed()
            if not self.is_stopped:
                os = self.observers[:]
                self.observers = []
                self.is_stopped = True

        if os:
            for observer in os:
                observer.on_completed()

    def _on_error_core(self, exception):
        """Notifies all subscribed observers with the exception.

        Keyword arguments:
        error -- The exception to send to all subscribed observers.
        """

        os = None
        with self.lock:
            self.check_disposed()
            if not self.is_stopped:
                os = self.observers[:]
                self.observers = []
                self.is_stopped = True
                self.exception = exception

        if os:
            for observer in os:
                observer.on_error(exception)

    def _on_next_core(self, value):
        """Notifies all subscribed observers with the value.

        Keyword arguments:
        value -- The value to send to all subscribed observers.
        """

        os = None
        with self.lock:
            self.check_disposed()
            if not self.is_stopped:
                os = self.observers[:]

        if os:
            # def action(_, __):
            # print('number of observers = {}, value={}'.format(len(os), value))
            for observer in os:
                observer.on_next(value)

            # self.scheduler.schedule(action)

    def dispose(self):
        """Unsubscribe all observers and release resources."""

        with self.lock:
            self.is_disposed = True
            self.observers = None
            if self.backpressure is not None:
                self.backpressure.dispose()
