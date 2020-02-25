from dataclasses import dataclass
from typing import Callable, List

from rxbp.flowable import Flowable
from rxbp.multicast.multicast import MultiCast


@dataclass
class ImperativeMultiCastBuild:
    blocking_flowable: Flowable
    output_selector: Callable[[Flowable], MultiCast]
    subjects: List
