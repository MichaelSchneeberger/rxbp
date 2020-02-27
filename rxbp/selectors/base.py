from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from rxbp.selectors.matchopresult import MatchOpResultMixin
from rxbp.subscriber import Subscriber


class Base(ABC):
    """
    A base is a property that determines if two objects match. They match either
    directly or by some selector function.
    """

    @dataclass
    class MatchedBaseMapping(MatchOpResultMixin):
        base: 'Base'

    @abstractmethod
    def get_name(self):
        ...

    @abstractmethod
    def get_selectors(
            self,
            other: 'Base',
            subscriber: Subscriber,
    ) -> Optional['Base.MatchedBaseMapping']:
        ...
