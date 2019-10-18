# from typing import Callable
#
# from rxbp.flowablebase import FlowableBase
# from rxbp.flowables.refcountflowable import RefCountFlowable
# from rxbp.scheduler import Scheduler
# from rxbp.observablesubjects.publishosubject import PublishOSubject
# from rxbp.subscriber import Subscriber
# from rxbp.subscription import Subscription
#
#
# class ShareFlowable(FlowableBase):
#     """ for internal use
#     """
#
#     def __init__(self, source: FlowableBase, func: Callable[[RefCountFlowable], FlowableBase]):
#         super().__init__()
#
#         self._source = source
#         self._func = func
#
#     def unsafe_subscribe(self, subscriber: Subscriber):
#         # def subject_gen(scheduler: Scheduler):
#         #     return PublishOSubject(scheduler=scheduler)
#
#         flowable = self._func(RefCountFlowable(self._source)) #, subject_gen=subject_gen))
#         subscription = flowable.unsafe_subscribe(subscriber)
#         return subscription
