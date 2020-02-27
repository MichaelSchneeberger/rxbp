from dataclasses import dataclass

from rxbp.observable import Observable


class SelectorMap:
    """
    A Selector is a function that maps the elements of a sequence with some base B_1 to the matched base B_matched

    * identity - map all elements one-to-one
    * observableselector - modify sequence by the selector
    """

    pass


class IdentitySelectorMap(SelectorMap):
    pass


@dataclass
class ObservableSelectorMap(SelectorMap):
    observable: Observable
