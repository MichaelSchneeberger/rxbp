from typing import Tuple

from dataclass_abc import dataclass_abc

from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler


@dataclass_abc
class MultiCastSubscriberImpl(MultiCastSubscriber):
    subscribe_schedulers: Tuple[TrampolineScheduler]
