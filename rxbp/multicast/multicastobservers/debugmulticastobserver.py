from dataclasses import dataclass
from traceback import FrameSummary
from typing import Callable, Any, List

from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.typing import MultiCastItem
from rxbp.scheduler import Scheduler
from rxbp.utils.tooperatorexception import to_operator_exception


@dataclass
class DebugMultiCastObserver(MultiCastObserver):
    source: MultiCastObserver
    on_next_func: Callable[[Any], None]
    on_completed_func: Callable[[], None]
    on_error_func: Callable[[Exception], None]
    stack: List[FrameSummary]
    # source_scheduler: Scheduler

    def on_next(self, item: MultiCastItem) -> None:
        # if self.source_scheduler.idle:
        #     raise Exception(to_operator_exception(
        #         message='source scheduler should be active',
        #         stack=self.stack,
        #     ))

        try:
            item = list(item)

            if len(item) == 0:
                return

            for elem in item:
                self.on_next_func(elem)
            self.source.on_next(item)

        except Exception as exc:
            self.on_error(exc)
            return

    def on_error(self, exc: Exception) -> None:
        self.on_error_func(exc)
        self.source.on_error(exc)

    def on_completed(self) -> None:
        self.on_completed_func()
        self.source.on_completed()