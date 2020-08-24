from rxbp.multicast.impl.liftedmulticastimpl import LiftedMultiCastImpl
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin


def init_lifted_multicast(
        underlying: MultiCastMixin,
        nested_layer: int,
):
    return LiftedMultiCastImpl(
        underlying=underlying,
        nested_layer=nested_layer,
        is_hot=True,
    )
