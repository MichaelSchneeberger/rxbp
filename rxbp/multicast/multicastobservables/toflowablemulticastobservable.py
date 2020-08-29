from dataclasses import dataclass

from rx.disposable import Disposable, SingleAssignmentDisposable, CompositeDisposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastobservers.toflowablemulticastobserver import ToFlowableMultiCastObserver
from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import Scheduler
from rxbp.subscriber import Subscriber


@dataclass
class FromMultiCastObservable(Observable):
    source: MultiCastObservable
    subscriber: Subscriber
    multicast_subscribe_scheduler: Scheduler

    def observe(self, observer_info: ObserverInfo) -> Disposable:
        inner_disposable = SingleAssignmentDisposable()

        observer_info = MultiCastObserverInfo(ToFlowableMultiCastObserver(
            observer=observer_info.observer,
            subscriber=self.subscriber,
            is_first=True,
            inner_disposable=inner_disposable,
        ))

        def subscribe_action(_, __):
            return CompositeDisposable(
                self.source.observe(observer_info),
                inner_disposable,
            )

        return self.multicast_subscribe_scheduler.schedule(subscribe_action)
