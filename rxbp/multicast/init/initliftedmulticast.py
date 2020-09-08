from typing import Tuple

from rxbp.multicast.impl.liftedmulticastimpl import LiftedMultiCastImpl
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler


def init_lifted_multicast(
        underlying: MultiCastMixin,
        lift_index: int,
):
    return LiftedMultiCastImpl(
        underlying=underlying,
        lift_index=lift_index,
    )
