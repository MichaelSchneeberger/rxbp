from rxbp.impl.flowableimpl import FlowableImpl
from rxbp.mixins.flowablebasemixin import FlowableBaseMixin


def init_flowable(
        underlying: FlowableBaseMixin,
):
    return FlowableImpl(
        underlying=underlying
    )
