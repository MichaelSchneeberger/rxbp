from typing import Callable

from rxbp.flowablebase import FlowableBase
from rxbp.flowables.bufferflowable import BufferFlowable
from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.scheduler import Scheduler
from rxbp.selectors.bases import SharedBase
from rxbp.subjects.cacheservefirstsubject import CacheServeFirstSubject
from rxbp.subscriber import Subscriber


class CacheServeFirstFlowable(FlowableBase):
    def __init__(self, source: FlowableBase, func: Callable[[RefCountFlowable], FlowableBase]):
        super().__init__()

        self._source = source
        self._func = func

    def unsafe_subscribe(self, subscriber: Subscriber) -> FlowableBase.FlowableReturnType:
        base = SharedBase(has_fan_out=True, prev_base=self._source.base)

        def subject_gen(scheduler: Scheduler):
            return CacheServeFirstSubject(scheduler=scheduler)

        flowable = self._func(RefCountFlowable(self._source, subject_gen=subject_gen, base=base))
        obs, selector = flowable.unsafe_subscribe(subscriber)
        return obs, selector
