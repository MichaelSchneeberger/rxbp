from rxbp.indexed.impl.indexedsubscriptionimpl import IndexedSubscriptionImpl
from rxbp.observable import Observable
from rxbp.selectors.baseandselectors import BaseAndSelectors


def init_indexed_subscription(
        observable: Observable,
        index: BaseAndSelectors,
):
    return IndexedSubscriptionImpl(
        observable=observable,
        index=index,
    )
