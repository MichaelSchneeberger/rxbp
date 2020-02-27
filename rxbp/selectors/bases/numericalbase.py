from dataclasses import dataclass

from rxbp.selectors.base import Base, BaseAndSelectorMaps
from rxbp.selectors.selectormap import IdentitySelectorMap
from rxbp.subscriber import Subscriber


@dataclass(frozen=True)
class NumericalBase(Base):
    num: int

    # def __init__(self, num: int):
    #     self.num = num

    def get_name(self):
        return f'{self.__class__.__name__}({self.num})'

    def get_base_and_selector_maps(self, other: Base, subscriber: Subscriber):
        if isinstance(other, NumericalBase) and self.num == other.num:
            return BaseAndSelectorMaps(
                left=IdentitySelectorMap(),
                right=IdentitySelectorMap(),
                base=self,
            )
        else:
            return None
