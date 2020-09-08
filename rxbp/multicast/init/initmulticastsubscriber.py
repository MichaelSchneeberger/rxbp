from typing import Tuple

from rxbp.multicast.impl.multicastsubscriberimpl import MultiCastSubscriberImpl
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler


def init_multicast_subscriber(
        subscribe_schedulers: Tuple[TrampolineScheduler, ...],
):
    return MultiCastSubscriberImpl(
        subscribe_schedulers=subscribe_schedulers,
    )
