from dataclasses import dataclass
from typing import Any

from rxbp.indexed.selectors.flowablebase import FlowableBase, FlowableBaseMatch
from rxbp.indexed.selectors.identityseqmapinfo import IdentitySeqMapInfo
from rxbp.subscriber import Subscriber


@dataclass(frozen=True)
class ObjectRefBase(FlowableBase):
    obj: Any

    def get_name(self):
        return f'{self.__class__.__name__}({self.obj})'

    def match_with(self, other: FlowableBase, subscriber: Subscriber):
        if isinstance(other, ObjectRefBase) and self.obj == other.obj:
            return FlowableBaseMatch(
                left=IdentitySeqMapInfo(),
                right=IdentitySeqMapInfo(),
                base=self,
            )
        else:
            return None