from rx.core.typing import Disposable, Scheduler
from rx.disposable import SingleAssignmentDisposable, CompositeDisposable
from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo


class DelaySubscriptionObservable(Observable):
    def __init__(self, source: Observable, subscribe_scheduler: Scheduler):
        self.source = source
        self.subscribe_scheduler = subscribe_scheduler

    def observe(self, observer_info: ObserverInfo) -> Disposable:
        d1 = SingleAssignmentDisposable()

        def action(_, __):
            d1.disposable = self.source.observe(observer_info)

        d2 = self.subscribe_scheduler.schedule(action)

        return CompositeDisposable(d1, d2)