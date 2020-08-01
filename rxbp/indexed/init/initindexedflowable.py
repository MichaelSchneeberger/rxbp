from rxbp.indexed.impl.indexedflowableimpl import IndexedFlowableImpl
from rxbp.indexed.mixins.indexedflowablemixin import IndexedFlowableMixin


def init_indexed_flowable(
        underlying: IndexedFlowableMixin,
):
    return IndexedFlowableImpl(
        underlying=underlying
    )
