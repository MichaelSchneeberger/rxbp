from dataclasses import replace

from dataclass_abc import dataclass_abc

from rxbp.scheduler import Scheduler
from rxbp.subscriber import Subscriber


@dataclass_abc
class SubscriberImpl(Subscriber):
    scheduler: Scheduler
    subscribe_scheduler: Scheduler

    def copy(self, **kwargs):
        return replace(self, **kwargs)
