from rxbp.selectors.base import Base, BaseAndSelectorMaps
from rxbp.selectors.selectormap import IdentitySelectorMap
from rxbp.subscriber import Subscriber


class PairwiseBase(Base):
    def __init__(self, underlying: Base):
        self.underlying = underlying

    def get_name(self):
        return f'PairwiseBase({self.underlying.get_name()})'

    def get_base_and_selector_maps(self, other: Base, subscriber: Subscriber):
        if isinstance(other, PairwiseBase):
            result: BaseAndSelectorMaps = self.underlying.get_base_and_selector_maps(other.underlying, subscriber=subscriber)

            # after pairing, one cannot be transformed into the other
            if isinstance(result, BaseAndSelectorMaps):
                if isinstance(result.left, IdentitySelectorMap) and isinstance(result.right, IdentitySelectorMap):
                    return result

        return None