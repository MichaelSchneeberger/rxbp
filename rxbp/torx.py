from datetime import datetime
from typing import Optional

from rx import Observable
from rx.core import typing
from rx.core.typing import AbsoluteTime, TState, Disposable, RelativeTime

from rxbp.ack import Continue
from rxbp.flowablebase import FlowableBase
from rxbp.observer import Observer
from rxbp.scheduler import SchedulerBase
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.subscriber import Subscriber


def to_rx(source: FlowableBase):
    """ Converts this observable to an rx.Observable

    :param scheduler:
    :return:
    """

    class FromFlowableObservable(Observable):
        def _subscribe_core(self, observer: typing.Observer, scheduler: typing.Scheduler = None):
            class RxBPScheduler(SchedulerBase):
                @property
                def now(self) -> datetime:
                    return scheduler.now

                def schedule(self, action: 'ScheduledAction', state: TState = None) -> Disposable:
                    return scheduler.schedule(action=action, state=state)

                def schedule_relative(self, duetime: RelativeTime, action: 'ScheduledAction',
                                      state: TState = None) -> Disposable:
                    return scheduler.schedule_relative(duetime=duetime, action=action, state=state)

                def schedule_absolute(self, duetime: AbsoluteTime, action: 'ScheduledAction',
                                      state: TState = None) -> Disposable:
                    return scheduler.schedule_absolute(duetime=duetime, action=action, state=state)

                def schedule_periodic(self, period: RelativeTime, action: 'ScheduledPeriodicAction',
                                      state: Optional[TState] = None) -> Disposable:
                    raise NotImplementedError

            class ToRxObserver(Observer):
                def on_next(self, v):
                    for e in v():
                        observer.on_next(e)
                    return Continue()

                def on_error(self, err):
                    observer.on_error(err)

                def on_completed(self):
                    observer.on_completed()

            current_thread_scheduler = TrampolineScheduler()
            scheduler_ = RxBPScheduler() if scheduler is not None else current_thread_scheduler
            subscriber = Subscriber(scheduler=scheduler_, subscribe_scheduler=current_thread_scheduler)
            return source.subscribe_(observer=ToRxObserver(), subscriber=subscriber)

    return FromFlowableObservable()