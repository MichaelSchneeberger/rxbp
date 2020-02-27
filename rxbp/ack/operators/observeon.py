from rx.core.typing import Scheduler

from rxbp.ack.mixins.ackmixin import AckMixin
from rxbp.ack.scheduledsingle import ScheduledSingle
from rxbp.ack.single import Single


def _observe_on(source: AckMixin, scheduler: Scheduler) -> AckMixin:
    class ObserveOnSingle(AckMixin):
        def subscribe(self, single: Single):
            return source.subscribe(ScheduledSingle(scheduler=scheduler, single=single))

    return ObserveOnSingle()
