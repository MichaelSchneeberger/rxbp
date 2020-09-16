from dataclasses import dataclass
from traceback import FrameSummary
from typing import List

from rxbp.indexed.selectors.flowablebase import FlowableBase, FlowableBaseMatch
from rxbp.indexed.selectors.identityseqmapinfo import IdentitySeqMapInfo
from rxbp.subscriber import Subscriber


@dataclass(frozen=True)
class NumericalBase(FlowableBase):
    num: int

    def get_name(self):
        return f'{self.__class__.__name__}({self.num})'

    def match_with(
            self,
            other: FlowableBase,
            subscriber: Subscriber,
            stack: List[FrameSummary],
    ):
        if isinstance(other, NumericalBase) and self.num == other.num:
            return FlowableBaseMatch(
                left=IdentitySeqMapInfo(),
                right=IdentitySeqMapInfo(),
                base=self,
            )
        else:
            return None
