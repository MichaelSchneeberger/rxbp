from abc import ABC, abstractmethod
from dataclasses import dataclass
from traceback import FrameSummary
from typing import Optional, List

from rxbp.indexed.selectors.seqmapinfopair import SeqMapInfoPair
from rxbp.subscriber import Subscriber


@dataclass
class FlowableBaseMatch(SeqMapInfoPair):
    # base for the new Flowable
    base: 'FlowableBase'


class FlowableBase(ABC):
    @abstractmethod
    def get_name(self) -> str:
        ...

    @abstractmethod
    def match_with(
            self,
            other: 'FlowableBase',
            subscriber: Subscriber,
            stack: List[FrameSummary],
    ) -> Optional[FlowableBaseMatch]:
        """
        determines if two Flowables bases match. If they match a FlowableBaseMatch
        is returned that contains the new base and two SeqMapInfo that map the this
        Flowable sequence and the other Flowable sequence to the new base.
        """

        ...
