from typing import Any, List

import rxbp
from rxbp.flowable import Flowable
from rxbp.stream.stream import Stream
from rxbp.stream.streambase import StreamBase


def return_value(val: Any):
    class FromObjectStream(StreamBase):
        def __init__(self, obj: Any):
            self._obj = obj

        @property
        def source(self) -> Flowable:
            return rxbp.return_value(self._obj)

    return Stream(FromObjectStream(obj=val))


# def from_flowable(val: Flowable):
#     return Stream(FromFlowableStream(source=val))


# def merge(streams: List[Stream]):
#     pass