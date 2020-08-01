from rxbp.multicast.impl.multicastimpl import MultiCastImpl
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin


def init_multicast(
        underlying: MultiCastMixin,
):
    return MultiCastImpl(underlying=underlying)
