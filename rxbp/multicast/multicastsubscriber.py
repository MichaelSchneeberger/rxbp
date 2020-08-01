from dataclasses import dataclass

from rxbp.scheduler import Scheduler


@dataclass
class MultiCastSubscriber:
    source_scheduler: Scheduler
    multicast_scheduler: Scheduler
