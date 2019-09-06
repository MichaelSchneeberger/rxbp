from typing import Callable

from rxbp.flowablebase import FlowableBase
from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.scheduler import Scheduler
from rxbp.observablesubjects.observablecacheservefirstsubject import ObservableCacheServeFirstSubject
from rxbp.selectors.bases import Base
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class CacheServeFirstFlowable(FlowableBase):
    def __init__(self, source: FlowableBase, func: Callable[[RefCountFlowable], Base]):
        super().__init__(base=source.base)

        self._source = source
        self._func = func

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        raise NotImplementedError
        # base = SharedBase(prev_base=self._source.base)

        def subject_gen(scheduler: Scheduler):
            return ObservableCacheServeFirstSubject(scheduler=scheduler)

        flowable = self._func(RefCountFlowable(self._source, subject_gen=subject_gen, base=base))
        obs, selector = flowable.unsafe_subscribe(subscriber)

        return obs, selector
