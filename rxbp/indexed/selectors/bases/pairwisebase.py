from dataclasses import dataclass
from traceback import FrameSummary
from typing import List

from rxbp.indexed.selectors.flowablebase import FlowableBase, FlowableBaseMatch
from rxbp.indexed.selectors.identityseqmapinfo import IdentitySeqMapInfo
from rxbp.subscriber import Subscriber


@dataclass(frozen=True)
class PairwiseBase(FlowableBase):
    underlying: FlowableBase

    def get_name(self):
        return f'PairwiseBase({self.underlying.get_name()})'

    def match_with(
            self,
            other: FlowableBase,
            subscriber: Subscriber,
            stack: List[FrameSummary],
    ):
        if isinstance(other, PairwiseBase):
            result: FlowableBaseMatch = self.underlying.match_with(
                other=other.underlying,
                subscriber=subscriber,
                stack=stack,
            )

            # after pairing, one cannot be transformed into the other
            if isinstance(result, FlowableBaseMatch):
                if isinstance(result.left, IdentitySeqMapInfo) and isinstance(result.right, IdentitySeqMapInfo):
                    return result

        return None