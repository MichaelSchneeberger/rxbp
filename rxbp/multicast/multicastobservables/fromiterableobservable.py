from dataclasses import dataclass
from typing import Iterable, Any

import rx
from rx.disposable import Disposable

from rxbp.mixins.schedulermixin import SchedulerMixin
from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber


@dataclass
class FromIterableObservable(MultiCastObservable):
    values: Iterable[Any]
    subscriber: MultiCastSubscriber
    scheduler_index: int

    def observe(self, observer_info: MultiCastObserverInfo) -> rx.typing.Disposable:
        def scheduler_action():
            def action(_, __):
                try:
                    observer_info.observer.on_next(self.values)
                    observer_info.observer.on_completed()
                except Exception as exc:
                    observer_info.observer.on_error(exc)
            return self.subscriber.subscribe_schedulers[self.scheduler_index].schedule(action)

        try:
            return self.subscriber.schedule_action(
                index=self.scheduler_index - 1,
                action=scheduler_action,
            )

        except Exception as exc:
            observer_info.observer.on_error(exc)
            return Disposable()
