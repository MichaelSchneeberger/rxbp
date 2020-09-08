from dataclasses import dataclass
from traceback import FrameSummary
from typing import Callable, Any, List

import rx
from rx.disposable import Disposable, CompositeDisposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastobservers.debugmulticastobserver import DebugMultiCastObserver
from rxbp.scheduler import Scheduler
from rxbp.utils.tooperatorexception import to_operator_exception


@dataclass
class DebugMultiCastObservable(MultiCastObservable):
    source: MultiCastObservable
    # multicast_scheduler: Scheduler
    # source_scheduler: Scheduler
    name: str
    on_next: Callable[[Any], None]
    on_completed: Callable[[], None]
    on_error: Callable[[Exception], None]
    on_observe: Callable[[MultiCastObserverInfo], None]
    on_dispose: Callable[[], None]
    stack: List[FrameSummary]

    def observe(self, observer_info: MultiCastObserverInfo) -> rx.typing.Disposable:
        self.on_observe(observer_info)

        # if self.multicast_scheduler.idle:
        #     raise Exception(to_operator_exception(
        #         message='observe method call should be scheduled on multicast scheduler',
        #         stack=self.stack,
        #     ))

        observer = DebugMultiCastObserver(
            source=observer_info.observer,
            on_next_func=self.on_next,
            on_completed_func=self.on_completed,
            on_error_func=self.on_error,
            stack=self.stack,
            # source_scheduler=self.source_scheduler,
        )

        # def action(_, __):
        #     observer.has_scheduled_next = True
        # self.multicast_scheduler.schedule(action)

        return CompositeDisposable(
            self.source.observe(observer_info.copy(
                observer=observer,
            )),
            Disposable(self.on_dispose),
        )
