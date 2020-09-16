from dataclasses import dataclass

from rx.disposable import Disposable, SingleAssignmentDisposable, CompositeDisposable

from rxbp.multicast.init.initmulticastobserverinfo import init_multicast_observer_info
from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastobservers.toflowablemulticastobserver import ToFlowableMultiCastObserver
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import Scheduler
from rxbp.subscriber import Subscriber


@dataclass
class FromMultiCastObservable(Observable):
    source: MultiCastObservable
    subscriber: Subscriber
    multicast_subscriber: MultiCastSubscriber
    lift_index: int

    def observe(self, observer_info: ObserverInfo) -> Disposable:
        def action():
            inner_disposable = SingleAssignmentDisposable()

            disposable = self.source.observe(
                observer_info=init_multicast_observer_info(
                    observer=ToFlowableMultiCastObserver(
                        observer=observer_info.observer,
                        multicast_subscriber=self.multicast_subscriber,
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

        try:
            return self.multicast_subscriber.schedule_action(
                action=action,
            )

        except Exception as exc:
            observer_info.observer.on_error(exc)
            observer_info.observer.on_completed()
