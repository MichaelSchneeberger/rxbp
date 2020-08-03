# from rxbp.mixins.flowablemixin import FlowableMixin
# from rxbp.scheduler import Scheduler
# from rxbp.subscriber import Subscriber
#
#
# class ExecuteOnFlowable(FlowableMixin):
#     def __init__(self, source: FlowableMixin, scheduler: Scheduler):
#         super().__init__()
#
#         self._source = source
#         self._scheduler = scheduler
#
#     def unsafe_subscribe(self, subscriber: Subscriber):
#         updated_subscriber = Subscriber(scheduler=self._scheduler,
#                                         subscribe_scheduler=subscriber.subscribe_scheduler)
#
#         return self._source.unsafe_subscribe(updated_subscriber)