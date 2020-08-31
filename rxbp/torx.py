from datetime import datetime
from typing import Optional

from rx import Observable
from rx.core import typing
from rx.core.typing import AbsoluteTime, TState, Disposable, RelativeTime, ScheduledAction, ScheduledPeriodicAction

from rxbp.acknowledgement.continueack import continue_ack
from rxbp.init.initsubscriber import init_subscriber
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.scheduler import SchedulerBase
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.typing import ElementType


def to_rx(source: FlowableMixin, batched: bool = None, subscribe_schduler: Scheduler = None):
    """ Converts this observable to an rx.Observable

    :param scheduler:
    :return:
    """

    class FromFlowableObservable(Observable):
        def _subscribe_core(self, observer: typing.Observer, scheduler: typing.Scheduler = None):
            class RxBPScheduler(SchedulerBase):
                def __init__(self, underlying):
                    super().__init__()

                    self.underlying = underlying

                def sleep(self, seconds: float) -> None:
                    pass

                @property
                def now(self) -> datetime:
                    return self.underlying.now

                @property
                def is_order_guaranteed(self) -> bool:
                    # unknown property, therefore select pessimistically
                    return False

                def schedule(self, action: ScheduledAction, state: TState = None) -> Disposable:
                    return self.underlying.schedule(action=action, state=state)

                def schedule_relative(self, duetime: RelativeTime, action: ScheduledAction,
                                      state: TState = None) -> Disposable:
                    return self.underlying.schedule_relative(duetime=duetime, action=action, state=state)

                def schedule_absolute(self, duetime: AbsoluteTime, action: ScheduledAction,
                                      state: TState = None) -> Disposable:
                    return self.underlying.schedule_absolute(duetime=duetime, action=action, state=state)

                def schedule_periodic(self, period: RelativeTime, action: ScheduledPeriodicAction,
                                      state: Optional[TState] = None) -> Disposable:
                    raise NotImplementedError

            class ToRxObserver(Observer):
                @property
                def is_volatile(self):
                    return False

                def on_next(self, elem: ElementType):
                    for e in elem:
                        observer.on_next(e)
                    return continue_ack

                def on_error(self, err):
                    observer.on_error(err)

                def on_completed(self):
                    observer.on_completed()

            to_rx_observer = ToRxObserver()

            if batched is True:
                def on_next(v):
                    batch = list(v())
                    observer.on_next(batch)
                    return continue_ack

                to_rx_observer.on_next = on_next

            trampoline_scheduler = subscribe_schduler or TrampolineScheduler()
            scheduler_ = RxBPScheduler(underlying=scheduler) if scheduler is not None else trampoline_scheduler
            subscriber = init_subscriber(scheduler=scheduler_, subscribe_scheduler=trampoline_scheduler)
            # observer_info = init_observer_info(observer=to_rx_observer)
            return source.subscribe(
                observer=to_rx_observer,
                subscribe_scheduler=subscriber.subscribe_scheduler,
                scheduler=subscriber.subscribe_scheduler,
            )

    return FromFlowableObservable()