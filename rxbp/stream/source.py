from typing import Any

import rxbp
from rxbp.flowable import Flowable
from rxbp.stream.stream import Stream
from rxbp.stream.streams.fromflowablestream import FromFlowableStream
from rxbp.stream.streams.fromobjectstream import FromObjectStream
from rxbp.stream.streambase import StreamBase
from rxbp.typing import BaseType


def return_value(val: Any):
    return Stream(FromObjectStream(obj=val))


def from_flowable(val: Flowable):
    return Stream(FromFlowableStream(source=val))
