from abc import ABC, abstractmethod
from typing import Tuple, Callable, Optional

import rx

from rxbp.schedulers.trampolinescheduler import TrampolineScheduler


class MultiCastSubscriberMixin(ABC):
    @property
    @abstractmethod
    def subscribe_schedulers(self) -> Tuple[TrampolineScheduler]:
        ...

    def schedule_action(
            self,
            action: Callable[[], Optional[rx.typing.Disposable]],
            index: int = None,
    ) -> rx.typing.Disposable:
        if index is None:
            index = len(self.subscribe_schedulers) - 1

        else:
            assert index < len(self.subscribe_schedulers), f'index "{index}" is out of range of "{len(self.subscribe_schedulers)}"'

        def inner_schedule_action(
                action: Callable[[], Optional[rx.typing.Disposable]],
                current_index: int,
        ):
            if current_index == index:
                def inner_action(_, __):
                    return action()

            else:
                def inner_action(_, __):
                    return inner_schedule_action(
                        current_index=current_index + 1,
                        action=action,
                    )

            with self.subscribe_schedulers[current_index].lock:
                if self.subscribe_schedulers[current_index].idle:
                    disposable = self.subscribe_schedulers[current_index].schedule(inner_action)
                    return disposable

                else:
                    return inner_action(None, None)

        return inner_schedule_action(
            current_index=0,
            action=action,
        )
