from rx.core.typing import Scheduler
from rxbp.ack.ackbase import AckBase
from rxbp.ack.scheduledsingle import ScheduledObserver
from rxbp.ack.single import Single


def _observe_on(source: AckBase, scheduler: Scheduler) -> AckBase:
    class ObserveOnSingle(AckBase):
        def subscribe(self, single: Single):
            return source.subscribe(ScheduledObserver(scheduler=scheduler, single=single))

    return ObserveOnSingle()
