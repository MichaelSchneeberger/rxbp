import types
from itertools import chain
from typing import Any, Callable

import rx
from rx.disposable import CompositeDisposable, Disposable

from rxbp.mixins.schedulermixin import SchedulerMixin
from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.typing import MultiCastItem


class LiftedMultiCastObservable(MultiCastObservable):
    def __init__(
            self,
            source: MultiCastObservable,
            func: Callable[[MultiCastObservable, Any], Any],
            scheduler: SchedulerMixin,
    ):
        super().__init__()

        self.source = source
        self.func = func
        self.scheduler = scheduler

        self.disposable = CompositeDisposable()

    class LiftedSingleObservable(MultiCastObservable):
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

        def observe(self, observer_info: MultiCastObserverInfo) -> rx.typing.Disposable:
            self.observer = observer_info.observer
            return self.disposable

    class LiftObserver(MultiCastObserver):
        def __init__(
                self,
                func: Callable[[Any, MultiCastObservable], Any],
                observer: MultiCastObserver,
                subscribe_scheduler: SchedulerMixin,
                disposable: CompositeDisposable,
        ):
            super().__init__()

            self.func = func
            self.observer = observer
            self.subscribe_scheduler = subscribe_scheduler

            self.is_first = True
            self.elements = []

            self.observable: LiftedMultiCastObservable.LiftedSingleObservable = None

            self.disposable = disposable

            # self.lock = threading.RLock()

        def on_next(self, item: MultiCastItem):
            if isinstance(item, list):
                first_elem = item[0]
                all_elem = item

            else:
                try:
                    first_elem = next(item)
                    all_elem = chain([first_elem], item)
                except StopIteration:
                    return

            # with self.lock:
            #     is_first = self.is_first
            #     self.is_first = False

            if self.is_first:
                self.is_first = False

                self.observable = LiftedMultiCastObservable.LiftedSingleObservable(
                    first=first_elem,
                    subscribe_scheduler=self.subscribe_scheduler,
                    disposable=self.disposable,
                )

                self.observer.on_next([self.func(self.observable, first_elem)])
                self.observer.on_completed()

                # observable didn't get subscribed
                if self.observable.observer is None:
                    def on_next_if_not_subscribed(self, val):
                        pass

                    self.on_next = types.MethodType(on_next_if_not_subscribed, self)

                    # if there is no inner subscriber, dispose the source
                    self.disposable.dispose()
                    return

            self.observable.observer.on_next(all_elem)

        def on_error(self, exc: Exception):
            # with self.lock:
            #     is_first = self.is_first
            #     self.is_first = False

            if self.is_first:
                self.observer.on_error(exc)
            else:
                self.observable.observer.on_error(exc)

        def on_completed(self):
            # with self.lock:
            #     is_first = self.is_first
            #     self.is_first = False

            if self.is_first:
                self.observer.on_completed()
            else:
                self.observable.observer.on_completed()

    def observe(self, observer_info: MultiCastObserverInfo) -> rx.typing.Disposable:

        observer = self.LiftObserver(
            func=self.func,
            observer=observer_info.observer,
            subscribe_scheduler=self.scheduler,
            disposable=self.disposable,
        )

        source_disposable = self.source.observe(
            observer_info.copy(observer=observer),
        )
        self.disposable.add(source_disposable)

        def dispose_func():

            if observer.observable is None:
                self.disposable.dispose()

            # if inner observable is subscribed, then it is the job of the inner subscriber
            # to dispose the source
            else:
                pass

        return Disposable(dispose_func)
