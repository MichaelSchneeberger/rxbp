from dataclasses import dataclass
from typing import Any

from rx.core import typing, abc
from rx.core.observer import AutoDetachObserver
from rx.disposable import Disposable
from rx.scheduler import CurrentThreadScheduler

from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.pausablebufferedobserver import PausableBufferedObserver
from rxbp.scheduler import Scheduler


@dataclass
class CreateObservable(Observable):
    source: typing.Subscription
    scheduler: Scheduler
    subscribe_scheduler: Scheduler

    def observe(self, observer_info: ObserverInfo):
        observer = PausableBufferedObserver(
            underlying=observer_info.observer,
            scheduler=self.scheduler,
            subscribe_scheduler=self.subscribe_scheduler,
        )

        auto_detach_observer = AutoDetachObserver(
            observer.on_next,
            observer.on_error,
            observer.on_completed
        )

        def fix_subscriber(subscriber):
            """Fixes subscriber to make sure it returns a Disposable instead
            of None or a dispose function"""
            if not hasattr(subscriber, 'dispose'):
                subscriber = Disposable(subscriber)

            return subscriber

        def set_disposable(_: abc.Scheduler = None, __: Any = None):
            try:
                subscriber = self.source(auto_detach_observer, self.subscribe_scheduler)
            except Exception as ex:  # By design. pylint: disable=W0703
                if not auto_detach_observer.fail(ex):
                    raise
            else:
                auto_detach_observer.subscription = fix_subscriber(subscriber)

        current_thread_scheduler = CurrentThreadScheduler.singleton()
        if current_thread_scheduler.schedule_required():
            current_thread_scheduler.schedule(set_disposable)
        else:
            set_disposable()

        # Hide the identity of the auto detach observer
        return Disposable(auto_detach_observer.dispose)
