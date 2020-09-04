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
    multicast_scheduler: Scheduler

    def observe(self, observer_info: ObserverInfo) -> Disposable:
        def flowable_subscribe(_, __):
            def multilink_subscribe(_, __):
                inner_disposable = SingleAssignmentDisposable()

                disposable = self.source.observe(
                    observer_info=MultiCastObserverInfo(
                        observer=ToFlowableMultiCastObserver(
                            observer=observer_info.observer,
                            subscriber=self.subscriber,
                            is_first=True,
                            inner_disposable=inner_disposable,
                        ),
                    ),
                )

                return CompositeDisposable(
                    disposable,
                    inner_disposable,
                )

            return self.multicast_scheduler.schedule(multilink_subscribe)
        return self.subscriber.subscribe_scheduler.schedule(flowable_subscribe)
