from typing import Callable

from rxbp.flowablebase import FlowableBase
from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.subscriber import Subscriber


class SharedFlowable(FlowableBase):
    def __init__(self, source: FlowableBase, func: Callable[[RefCountFlowable], FlowableBase]):
        super().__init__()

        self._source = source
        self._func = func

    def unsafe_subscribe(self, subscriber: Subscriber) -> FlowableBase.FlowableReturnType:
        flowable = self._func(RefCountFlowable(self._source))
        obs, selector = flowable.unsafe_subscribe(subscriber)
        return obs, selector
