# from typing import Callable
#
# from rxbp.flowablebase import FlowableBase
# from rxbp.flowables.refcountflowable import RefCountFlowable
# from rxbp.scheduler import Scheduler
# from rxbp.observablesubjects.cacheservefirstosubject import CacheServeFirstOSubject
# from rxbp.selectors.bases import Base
# from rxbp.subscriber import Subscriber
# from rxbp.subscription import Subscription
#
#
# class CacheServeFirstFlowable(FlowableBase):    # todo: remove?
#     def __init__(self, source: FlowableBase, func: Callable[[RefCountFlowable], FlowableBase]):
#         super().__init__()
#
#         self._source = source
#         self._func = func
#
#     def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
#         def subject_gen(scheduler: Scheduler):
#             return CacheServeFirstOSubject(scheduler=scheduler)
#
#         flowable = self._func(RefCountFlowable(self._source, subject_gen=subject_gen))
#         subscription = flowable.unsafe_subscribe(subscriber)
#         return subscription
