from rxbp.impl.flowableimpl import FlowableImpl
from rxbp.mixins.flowablebasemixin import FlowableBaseMixin
from rxbp.multicast.impl.multicastflowableimpl import MultiCastFlowableImpl


def init_multicast_flowable(
        underlying: FlowableBaseMixin,
):
    return MultiCastFlowableImpl(
        underlying=underlying
    )
