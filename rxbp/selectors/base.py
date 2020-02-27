from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, List

from rxbp.selectors.matchopresult import SelectorMaps
from rxbp.subscriber import Subscriber


@dataclass
class BaseAndSelectorMaps(SelectorMaps):
    base: 'Base'


class Base(ABC):
    """
    A base is a property that determines if two objects match. They match either
    directly or by some selector function.
    """

    # @abstractmethod
    # def get_name(self):
    #     ...

    @abstractmethod
    def get_base_and_selector_maps(
            self,
            other: 'Base',
            subscriber: Subscriber,
    ) -> Optional[BaseAndSelectorMaps]:
        ...
