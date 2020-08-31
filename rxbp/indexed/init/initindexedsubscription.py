from rxbp.indexed.impl.indexedsubscriptionimpl import IndexedSubscriptionImpl
from rxbp.observable import Observable
from rxbp.indexed.selectors.flowablebaseandselectors import FlowableBaseAndSelectors


def init_indexed_subscription(
        observable: Observable,
        index: FlowableBaseAndSelectors,
):
    return IndexedSubscriptionImpl(
        observable=observable,
        index=index,
    )
