from dataclasses import dataclass

from rxbp.selectors.base import Base, BaseAndSelectorMaps
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.selectors.selectormap import IdentitySelectorMap
from rxbp.subscriber import Subscriber


@dataclass(frozen=True)
class TestBase(Base):
    underlying: BaseAndSelectors

    def get_base_and_selector_maps(self, other: Base, subscriber: Subscriber):
        if isinstance(other, TestBase):
            result = self.underlying.get_selectors(other.underlying, subscriber)

            if result is None:
                return None

            return BaseAndSelectorMaps(
                left=result.left,
                right=result.right,
                base=TestBase(result.base_selectors),
            )
        else:
            return None