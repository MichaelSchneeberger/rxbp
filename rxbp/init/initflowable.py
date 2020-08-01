from rxbp.impl.flowableimpl import FlowableImpl
from rxbp.mixins.flowablemixin import FlowableMixin


def init_flowable(
        underlying: FlowableMixin,
):
    return FlowableImpl(
        underlying=underlying
    )
