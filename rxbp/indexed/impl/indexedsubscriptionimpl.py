from dataclasses import replace

from dataclass_abc import dataclass_abc

from rxbp.indexed.indexedsubscription import IndexedSubscription
from rxbp.observable import Observable
from rxbp.indexed.selectors.flowablebaseandselectors import FlowableBaseAndSelectors


@dataclass_abc
class IndexedSubscriptionImpl(IndexedSubscription):
    observable: Observable
    index: FlowableBaseAndSelectors

    def copy(self, **kwargs):
        return replace(self, **kwargs)
