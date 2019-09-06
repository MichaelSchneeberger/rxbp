from abc import abstractmethod, ABC

from rxbp.observable import Observable
from rxbp.subscriber import Subscriber


class Selector:
    pass


class IdentitySelector(Selector):
    pass


class ObservableSelector(Selector):
    def __init__(self, observable: Observable):
        self.observable = observable


class SelectorResult:
    pass


class SelectorFound(SelectorResult):
    def __init__(self, left: Selector, right: Selector):
        self.left = left
        self.right = right


class NoSelectorFound(SelectorResult):
    pass


class GetSelectorMixin(ABC):
    @abstractmethod
    def get_selectors(
            self,
            other: 'GetSelectorMixin',
            subscriber: Subscriber,
    ) -> SelectorResult:
        ...
