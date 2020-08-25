from dataclasses import replace

from dataclass_abc import dataclass_abc

from rxbp.indexed.indexedsubscription import IndexedSubscription
from rxbp.observable import Observable
from rxbp.selectors.baseandselectors import BaseAndSelectors


@dataclass_abc
class IndexedSubscriptionImpl(IndexedSubscription):
    observable: Observable
    index: BaseAndSelectors

    def copy(self, **kwargs):
        return replace(self, **kwargs)
