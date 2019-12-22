from rxbp.flowablebase import FlowableBase
from rxbp.scheduler import Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.subscriber import Subscriber


class UseSubscribeSchedulerFlowable(FlowableBase):
    def __init__(self, source: FlowableBase, scheduler: Scheduler = None):
        super().__init__()

        self._source = source
        self._scheduler = scheduler

    def unsafe_subscribe(self, subscriber: Subscriber):
        scheduler = self._scheduler or TrampolineScheduler()

        updated_subscriber = Subscriber(scheduler=scheduler,
                                        subscribe_scheduler=scheduler)

        return self._source.unsafe_subscribe(updated_subscriber)
