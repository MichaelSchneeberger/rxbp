from dataclasses import dataclass

from rxbp.selectors.selector import Selector


@dataclass
class MatchOpResultMixin:
    left: Selector
    right: Selector


# @dataclass
# class MatchedBaseMap(MatchOpResult):
#     left: Selector
#     right: Selector
#     base: Optional[Base]
#     # selectors: Dict[Base, Observable] = None
#
#     def copy(self, **kwargs):
#         return dataclasses.replace(self, **kwargs)
#
#
# @dataclass
# class MatchedBaseMapWithSelectors(MatchedBaseMap):
#     selectors: Optional[Dict[Base, Observable]]
#
#
# class CouldNotMatch(MatchOpResult):
#     pass