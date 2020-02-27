import dataclasses
from dataclasses import dataclass

from rxbp.observable import Observable
from rxbp.selectors.baseandselectors import BaseAndSelectors


@dataclass
class Subscription:
    info: BaseAndSelectors

    observable: Observable

    def copy(self, **kwargs):
        return dataclasses.replace(self, **kwargs)
