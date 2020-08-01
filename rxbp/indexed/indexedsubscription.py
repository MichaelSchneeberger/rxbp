from abc import ABC

from rxbp.indexed.mixins.indexedsubscriptionmixin import IndexedSubscriptionMixin


class IndexedSubscription(IndexedSubscriptionMixin, ABC):
    pass
