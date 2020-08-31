from dataclasses import dataclass

from rxbp.indexed.selectors.seqmapinfo import SeqMapInfo


@dataclass
class SeqMapInfoPair:
    left: SeqMapInfo
    right: SeqMapInfo
