from dataclasses import dataclass

from rxbp.selectors.selectormap import SelectorMap


@dataclass
class SelectorMaps:
    left: SelectorMap
    right: SelectorMap
