import types
from typing import Callable, Any

from rx.disposable import CompositeDisposable, Disposable

from rxbp.ack.stopack import stop_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.typing import ElementType


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
        observer = observer_info.observer

        class DoActionObserver(Observer):
            def on_next(self, elem: ElementType):
                return observer.on_next(elem)

            def on_error(self, exc):
                observer.on_error(exc)

            def on_completed(self):
                observer.on_completed()

        do_action_observer = DoActionObserver()

        if self.on_next is not None:
            def on_next(_, val):
                if not isinstance(val, list):
                    try:
                        val = list(val)
                    except Exception as exc:
                        self.on_error(exc)
                        return stop_ack

                for item in val:
                    self.on_next(item)
                return observer.on_next(val)

            do_action_observer.on_next = types.MethodType(on_next, do_action_observer)

        if self.on_completed is not None:
            def on_completed(_):
                self.on_completed()
                return observer.on_completed()

            do_action_observer.on_completed = types.MethodType(on_completed, do_action_observer)

        if self.on_error is not None:
            def on_error(_, exc):
                self.on_error(exc)
                return observer.on_error(exc)

            do_action_observer.on_error = types.MethodType(on_error, do_action_observer)

        observer_info = ObserverInfo(do_action_observer, is_volatile=observer_info.is_volatile)
        disposable = self.source.observe(observer_info)

        if self.on_disposed is None:
            return disposable
        else:
            return CompositeDisposable(disposable, Disposable(self.on_disposed))
