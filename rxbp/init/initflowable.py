from rxbp.impl.flowableimpl import FlowableImpl
from rxbp.mixins.flowablemixin import FlowableMixin


def init_flowable(
        underlying: FlowableMixin,
        is_hot: bool = None
):
    if is_hot is None:
        is_hot = False

    return FlowableImpl(
        underlying=underlying,
        is_hot=is_hot,
    )
