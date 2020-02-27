import threading
import types
from typing import Any, Callable, Optional

import rx
from rx import Observable
from rx.core import Observer
from rx.core.typing import Scheduler
from rx.disposable import Disposable, CompositeDisposable


class LiftObservable(Observable):
    def __init__(
            self,
            source: Observable,
            func: Callable[[Observable, Any], Any],
            scheduler: Scheduler,
    ):
        super().__init__()

        self.source = source
        self.func = func
        self.scheduler = scheduler

        self.disposable = CompositeDisposable()

    class LiftedSingleObservable(Observable):
        def __init__(
                self,
                first,
                subscribe_scheduler,
                disposable: CompositeDisposable,
        ):
            super().__init__()

            self.first = first
            self.subscribe_scheduler = subscribe_scheduler
            self.disposable = disposable

            self.observer = None

        def _subscribe_core(self,
                            observer: rx.typing.Observer,
                            scheduler: Optional[rx.typing.Scheduler] = None
                            ) -> rx.typing.Disposable:
            self.observer = observer

            return self.disposable

    class LiftObserver(Observer):  # replace by TestObserver?
        def __init__(
                self,
                func: Callable[[Any, Observable], Any],
                observer: Observer,
                subscribe_scheduler: Scheduler,
                disposable: CompositeDisposable,
        ):
            super().__init__()

            self.func = func
            self.observer = observer
            self.subscribe_scheduler = subscribe_scheduler

            self.is_first = True
            self.elements = []

            self.observable: LiftObservable.LiftedSingleObservable = None

            self.disposable = disposable

            self.lock = threading.RLock()

        def on_next(self, val):
            with self.lock:
                is_first = self.is_first
                self.is_first = False

            if is_first:

                self.observable = LiftObservable.LiftedSingleObservable(
                    val,
                    self.subscribe_scheduler,
                    disposable=self.disposable,
                )

                value = self.func(self.observable, val)

                self.observer.on_next(value)
                self.observer.on_completed()

                # observable didn't get subscribed
                if self.observable.observer is None:
                    # print('nobody subscribed')

                    def on_next_if_not_subscribed(self, val):
                        pass

                    self.on_next = types.MethodType(on_next_if_not_subscribed, self)

                    # if there is no inner subscriber, dispose the source
                    self.disposable.dispose()
                    return

            self.observable.observer.on_next(val)

            #     self.elements.append(val)
            #
            #     def action(_, __):
            #         while True:
            #
            #             has_elem = True
            #             with self.lock:
            #                 if self.elements:
            #                     val = self.elements.pop(0)
            #                 else:
            #                     has_elem = False
            #
            #             if has_elem:
            #                 self.observable.observer.on_next(val)
            #
            #             else:
            #                 break
            #
            #     schedule_disposable = self.subscribe_scheduler.schedule(action)
            #     self.disposable.add(schedule_disposable)
            #
            # else:
            #     has_elem = True
            #     with self.lock:
            #         if self.elements:
            #             self.elements.append(val)
            #         else:
            #             has_elem = False
            #
            #     if not has_elem:
            #         def on_next_after_all_sent(self, val):
            #             # if self.observable.observer is not None:
            #             self.observable.observer.on_next(val)
            #
            #         self.on_next = types.MethodType(on_next_after_all_sent, self)
            #         on_next_after_all_sent(self, val)

        def on_error(self, exc: Exception):
            with self.lock:
                is_first = self.is_first
                self.is_first = False

            if is_first:
                self.observer.on_error(exc)
            else:
                self.observable.observer.on_error(exc)
            #     def action(_, __):
            #         self.observable.observer.on_error(exc)
            #
            #     self.subscribe_scheduler.schedule(action)

        def on_completed(self):
            with self.lock:
                is_first = self.is_first
                self.is_first = False

            if is_first:
                self.observer.on_completed()
            else:
                self.observable.observer.on_completed()
            #     def action(_, __):
            #         self.observable.observer.on_completed()
            #
            #     self.subscribe_scheduler.schedule(action)

    def _subscribe_core(
            self,
            observer: rx.typing.Observer,
            scheduler: Optional[rx.typing.Scheduler] = None
    ) -> rx.typing.Disposable:

        observer = self.LiftObserver(
            func=self.func,
            observer=observer,
            subscribe_scheduler=self.scheduler,
            disposable=self.disposable,
        )

        source_disposable = self.source.subscribe(observer=observer, scheduler=scheduler)
        self.disposable.add(source_disposable)

        def dispose_func():

            if observer.observable is None:
                self.disposable.dispose()

            # if inner observable is subscribed, then it is the job of the inner subscriber
            # to dispose the source
            else:
                pass

        return Disposable(dispose_func)
