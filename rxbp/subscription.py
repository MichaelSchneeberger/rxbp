import dataclasses
from dataclasses import dataclass

from rxbp.observable import Observable
from rxbp.selectors.baseselectorstuple import BaseSelectorsTuple


@dataclass
class Subscription:
    info: BaseSelectorsTuple

    observable: Observable

    def copy(self, **kwargs):
        return dataclasses.replace(self, **kwargs)
