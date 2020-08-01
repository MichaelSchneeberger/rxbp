from rxbp.impl.flowableimpl import FlowableImpl
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.multicast.impl.multicastflowableimpl import MultiCastFlowableImpl


def init_multicast_flowable(
        underlying: FlowableMixin,
):
    return MultiCastFlowableImpl(
        underlying=underlying
    )
