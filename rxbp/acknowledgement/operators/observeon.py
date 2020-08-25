from rx.core.typing import Scheduler

from rxbp.acknowledgement.ack import Ack
from rxbp.acknowledgement.scheduledsingle import ScheduledSingle
from rxbp.acknowledgement.single import Single


def _observe_on(source: Ack, scheduler: Scheduler) -> Ack:
    class ObserveOnSingle(Ack):
        def subscribe(self, single: Single):
            return source.subscribe(ScheduledSingle(scheduler=scheduler, single=single))

    return ObserveOnSingle()
