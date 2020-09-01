from dataclasses import dataclass

from rxbp.schedulers.trampolinescheduler import TrampolineScheduler


@dataclass
class MultiCastSubscriber:
    source_scheduler: TrampolineScheduler
    multicast_scheduler: TrampolineScheduler
