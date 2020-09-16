from rxbp.impl.sharedflowableimpl import SharedFlowableImpl
from rxbp.indexed.impl.indexedsharedflowableimpl import IndexedSharedFlowableImpl
from rxbp.indexed.mixins.indexedflowablemixin import IndexedFlowableMixin
from rxbp.mixins.flowablemixin import FlowableMixin


def init_indexed_shared_flowable(
        underlying: IndexedFlowableMixin,
):
    return IndexedSharedFlowableImpl(
        underlying=underlying,
    )
