from typing import Any

from rxbp.flowable import Flowable
from rxbp.stream.fromflowablestream import FromFlowableStream
from rxbp.stream.fromobjectstream import FromObjectStream
from rxbp.stream.stream import Stream


def return_value(val: Any):
    return Stream(FromObjectStream(obj=val))


def from_flowable(val: Flowable):
    return Stream(FromFlowableStream(source=val))
