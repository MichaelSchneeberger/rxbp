from rxbp.impl.sharedflowableimpl import SharedFlowableImpl
from rxbp.mixins.flowablemixin import FlowableMixin


def init_shared_flowable(
        underlying: FlowableMixin,
):
    return SharedFlowableImpl(
        underlying=underlying,
    )
