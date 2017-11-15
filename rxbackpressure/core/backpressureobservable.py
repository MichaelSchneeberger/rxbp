import types
from abc import abstractmethod

from rx import config
from rx.concurrency import current_thread_scheduler

from rxbackpressure.core.anonymousbackpressureobserver import AnonymousBackpressureObserver
from rxbackpressure.core.autodetachbackpressureobserver import AutoDetachBackpressureObserver


class BackpressureObservable:
    """Represents a push-style collection."""

    def __init__(self):
        # self._subscribe_backpressure_func = subscribe_backpressure_func
        self.lock = config["concurrency"].RLock()

        if hasattr(self, '_methods'):
            # Deferred instance method assignment
            for name, method in self._methods:
                setattr(self, name, types.MethodType(method, self))

    def subscribe(self, on_next=None, on_error=None, on_completed=None, observer=None, subscribe_bp=None):
        # Accept observer as first parameter
        # if isinstance(on_next, BackpressureObserver):
        #     observer = on_next
        # elif hasattr(on_next, "on_next") and callable(on_next.on_next):
        #     observer = on_next
        # elif not observer:
        # if isinstance(on_next, BackpressureObserver):
        observer = AnonymousBackpressureObserver(subscribe_bp=subscribe_bp,
                                                 on_next=on_next, on_error=on_error, on_completed=on_completed,
                                                 observer=observer)

        auto_detach_observer = AutoDetachBackpressureObserver(observer)

        def set_disposable(scheduler=None, value=None):
            # try:
            subscriber = self._subscribe_core(auto_detach_observer)
            # except Exception as ex:
            #     if not auto_detach_observer.fail(ex):
            #         raise
            auto_detach_observer.disposable = subscriber

        if current_thread_scheduler.schedule_required():
            current_thread_scheduler.schedule(set_disposable)
        else:
            set_disposable()

        # Hide the identity of the auto detach observer
        # return Disposable.create(auto_detach_observer.dispose)
        return auto_detach_observer

    @abstractmethod
    def _subscribe_core(self, observer):
        return NotImplemented