from rxbp.flowablebase import FlowableBase
from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class SubscribeOnFlowable(FlowableBase):
    def __init__(self, source: FlowableBase, scheduler: Scheduler = None):
        super().__init__()

        self._source = source
        self._scheduler = scheduler

    def unsafe_subscribe(self, subscriber: Subscriber):
        scheduler = self._scheduler or TrampolineScheduler()

        updated_subscriber = Subscriber(scheduler=scheduler,
                                        subscribe_scheduler=scheduler)

        subscription = self._source.unsafe_subscribe(updated_subscriber)

        class SubscribeOnObservable(Observable):
            def observe(_, observer_info: ObserverInfo):
                def action(_, __):
                    return subscription.observable.observe(observer_info)

                disposable = scheduler.schedule(action)

                return disposable

        observable = SubscribeOnObservable()

        return Subscription(info=subscription.info, observable=observable)