import types
from abc import abstractmethod

from rx import config
from rx.concurrency import current_thread_scheduler

from rxbackpressure.core.anonymousbackpressureobserver import AnonymousBackpressureObserver
from rxbackpressure.core.autodetachbackpressureobserver import AutoDetachBackpressureObserver


class BackpressureObservable:
    """Represents a pull-style collection."""

    def __init__(self):
        self.lock = config["concurrency"].RLock()

        if hasattr(self, '_methods'):
            # Deferred instance method assignment
            for name, method in self._methods:
                setattr(self, name, types.MethodType(method, self))

    def subscribe(self, on_next=None, on_error=None, on_completed=None, observer=None, subscribe_bp=None,
                  scheduler=None):
        observer = AnonymousBackpressureObserver(subscribe_bp=subscribe_bp,
                                                 on_next=on_next, on_error=on_error, on_completed=on_completed,
                                                 observer=observer)

        # if complete, then automatically dispose observer
        auto_detach_observer = AutoDetachBackpressureObserver(observer)

        def set_disposable(_=None, __=None):
            # try:
            subscriber = self._subscribe_core(auto_detach_observer, scheduler=scheduler)
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
    def _subscribe_core(self, observer, scheduler=None):
        return NotImplemented