from typing import Callable, Any

from rx.disposable import CompositeDisposable, Disposable

from rxbp.acknowledgement.stopack import stop_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo


class DoActionObservable(Observable):
    def __init__(
            self,
            source: Observable,
            on_next: Callable[[Any], None] = None,
            on_completed: Callable[[], None] = None,
            on_error: Callable[[Exception], None] = None,
            on_disposed: Callable[[], None] = None,
    ):
        self.source = source
        self.on_next = on_next
        self.on_completed = on_completed
        self.on_error = on_error
        self.on_disposed = on_disposed

    def observe(self, observer_info: ObserverInfo):
        if self.on_error is not None:
            def on_error(exc):
                self.on_error(exc)
                return observer_info.observer.on_error(exc)

        else:
            on_error = observer_info.observer.on_error

        if self.on_completed is not None:
            def on_completed():
                self.on_completed()
                return observer_info.observer.on_completed()

        else:
            on_completed = observer_info.observer.on_completed

        if self.on_next is not None:
            def on_next(val):
                if not isinstance(val, list):
                    try:
                        val = list(val)
                    except Exception as exc:
                        on_error(exc)
                        return stop_ack

                for item in val:
                    self.on_next(item)

                return observer_info.observer.on_next(val)

        else:
            on_next = observer_info.observer.on_next

        observer = type(
            'DoActionObserver',
            (Observer, object),
            {'on_next': on_next, 'on_completed': on_completed, 'on_error': on_error},
        )

        disposable = self.source.observe(
            observer_info=observer_info.copy(observer=observer),
        )

        if self.on_disposed is None:
            return disposable
        else:
            return CompositeDisposable(disposable, Disposable(self.on_disposed))
