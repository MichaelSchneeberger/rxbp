from typing import Any

from rxbp.selectors.base import Base, BaseAndSelectorMaps
from rxbp.selectors.selectormap import IdentitySelectorMap
from rxbp.subscriber import Subscriber


class ObjectRefBase(Base):
    def __init__(self, obj: Any = None):
        self.obj = obj or self

    def get_name(self):
        return f'{self.__class__.__name__}({self.obj})'

    def get_base_and_selector_maps(self, other: Base, subscriber: Subscriber):
        if isinstance(other, ObjectRefBase) and self.obj == other.obj:
            return BaseAndSelectorMaps(
                left=IdentitySelectorMap(),
                right=IdentitySelectorMap(),
                base=self,
            )
        else:
            return None