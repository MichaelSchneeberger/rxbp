from typing import Callable

import rx
from rxbp.flowablebase import FlowableBase
from rxbp.flowables.bufferflowable import BufferFlowable
from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.selectors.bases import SharedBase
from rxbp.subjects.cacheservefirstsubject import CacheServeFirstSubject
from rxbp.subscriber import Subscriber
from rxbp.typing import ElementType


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

    # def unsafe_subscribe(self, subscriber: Subscriber) -> FlowableBase.FlowableReturnType:
    #     base = SharedBase(has_fan_out=True, prev_base=self._source.base)
    #
    #     def subject_gen(scheduler: Scheduler):
    #         return CacheServeFirstSubject(scheduler=scheduler)
    #
    #     obs, _ = self._source.unsafe_subscribe(subscriber)
    #
    #     source = self
    #
    #     class DelayObservable(Observable):
    #         def observe(self, observer: Observer):
    #             class DelayObserver(Observer):
    #                 def on_next(self, elem: ElementType):
    #                     source_flowable = ConcatFlowable()
    #
    #                     flowable = source._func(RefCountFlowable(self._source, subject_gen=subject_gen, base=base))
    #                     obs, selector = flowable.unsafe_subscribe(subscriber)
    #                     disposable.disposable = d
    #
    #                 def on_error(self, exc: Exception):
    #                     pass
    #
    #                 def on_completed(self):
    #                     pass
    #
    #             disposable = obs.observe(DelayObserver())
    #             return disposable
    #
    #     return DelayObservable(), {}
