from dataclasses import dataclass

from rx.disposable import CompositeDisposable

from rxbp.acknowledgement.ack import Ack
from rxbp.acknowledgement.continueack import continue_ack
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.typing import MultiCastItem
from rxbp.observer import Observer


@dataclass
class FromFlowableMultiCastObserver(Observer):
    next_observer: MultiCastObserver
    subscriber: MultiCastSubscriber
    composite_disposable: CompositeDisposable()

    def on_next(self, elem: MultiCastItem) -> Ack:
        def scheduler_action():
            def action(_, __):
                try:
                    self.next_observer.on_next(elem)
                    self.next_observer.on_completed()
                except Exception as exc:
                    self.next_observer.on_error(exc)
            return self.subscriber.subscribe_schedulers[1].schedule(action)

        disposable = self.subscriber.schedule_action(
            index=0,
            action=scheduler_action,
        )
        self.composite_disposable.add(disposable)

        return continue_ack

    def on_error(self, exc: Exception) -> None:
        self.next_observer.on_error(exc)

    def on_completed(self) -> None:
        self.next_observer.on_completed()