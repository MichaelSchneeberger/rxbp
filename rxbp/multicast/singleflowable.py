from dataclasses import dataclass

from rxbp.flowable import Flowable


@dataclass
class SingleFlowable:
    source: Flowable