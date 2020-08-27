from rxbp.multicast.impl.notliftedmulticastimpl import NotLiftedMultiCastImpl
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin


def init_multicast(
        underlying: MultiCastMixin,
):
    return NotLiftedMultiCastImpl(
        underlying=underlying,
        # is_hot_on_subscribe=False,
        nested_layer=0,
    )
