from typing import Any, Callable, Optional

import rx
from rx import Observable
from rx.core import Observer
from rx.core.typing import Scheduler
from rx.disposable import Disposable


class LiftObservable(Observable):
    def __init__(
            self,
            source: Observable,
            func: Callable[[Any, Observable], Any],
            subscribe_scheduler: Scheduler,
    ):
        super().__init__()

        self.source = source
        self.func = func
        self.subscribe_scheduler = subscribe_scheduler

    class InnerObservable(Observable):
        def __init__(self, first, subscribe_scheduler):
            super().__init__()

            self.first = first
            self.subscribe_scheduler = subscribe_scheduler

            self.observer = None

        def _subscribe_core(self,
                        observer: rx.typing.Observer,
                        scheduler: Optional[rx.typing.Scheduler] = None
                        ) -> rx.typing.Disposable:
            self.observer = observer

            # def action(_, __):
            #     self.observer.on_next(self.first)
            #
            # return self.subscribe_scheduler.schedule(action)

    class LiftObserver(Observer):       # replace by TestObserver?
        def __init__(
                self,
                func: Callable[[Any, Observable], Any],
                observer: Observer,
                subscribe_scheduler: Scheduler,
        ):
            super().__init__()

            self.func = func
            self.observer = observer
            self.subscribe_scheduler = subscribe_scheduler

            self.is_first = True

            self.observable: LiftObservable.InnerObservable = None

        def on_next(self, val):
            if self.is_first:
                self.is_first = False

                self.observable = LiftObservable.InnerObservable(val, self.subscribe_scheduler)
                observable = self.func(val, self.observable)
                disposable = observable.subscribe(self.observer)
                # _ = self.observer.on_next(outer_val)

            # else:
            def action(_, __):

                if self.observable.observer is not None:
                    self.observable.observer.on_next(val)
                else:
                    # observable didn't get subscribed
                    pass

            self.subscribe_scheduler.schedule(action)

        def on_error(self, exc: Exception):
            if self.is_first:
                self.observer.on_error(exc)
            else:
                def action(_, __):

                    if self.observable.observer is not None:
                        self.observable.observer.on_error(exc)
                    else:
                        # observable didn't get subscribed
                        pass

                self.subscribe_scheduler.schedule(action)

        def on_completed(self):
            if self.is_first:
                self.observer.on_completed()
            else:
                def action(_, __):

                    if self.observable.observer is not None:
                        self.observable.observer.on_completed()
                    else:
                        # observable didn't get subscribed
                        pass

                self.subscribe_scheduler.schedule(action)

    def _subscribe_core(self,
                        observer: rx.typing.Observer,
                        scheduler: Optional[rx.typing.Scheduler] = None
                        ) -> rx.typing.Disposable:

        observer = self.LiftObserver(func=self.func, observer=observer, subscribe_scheduler=self.subscribe_scheduler)
        disposable = self.source.subscribe(observer=observer, scheduler=scheduler)
        return Disposable()
