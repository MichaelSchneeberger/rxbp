from dataclasses import dataclass

from rxbp.acknowledgement.acksubject import AckSubject
from rxbp.acknowledgement.continueack import ContinueAck
from rxbp.acknowledgement.stopack import StopAck, stop_ack
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.typing import ElementType


@dataclass
class ObserveOnObserver(Observer):
    observer: Observer
    scheduler: Scheduler

    def __post_init__(self):
        self.trampoline = TrampolineScheduler()

        if self.scheduler.is_order_guaranteed:
            def schedule_func(action):
                self.scheduler.schedule(action)

        # if the order of schedule actions cannot be guaranteed
        # by the scheduler, then use a TrampolineScheduler on top
        # of the scheduler to guarantee the order
        else:
            def schedule_func(action):
                def action_on_scheduler(_, __):
                    self.trampoline.schedule(action)
                self.scheduler.schedule(action_on_scheduler)

        self.schedule_func = schedule_func

    def on_next(self, elem: ElementType):
        ack_subject = AckSubject()

        if isinstance(elem, list):
            elem = elem
        else:
            try:
                elem = list(elem)
            except Exception as exc:
                self.observer.on_error(exc)
                return stop_ack

        def action(_, __):
            inner_ack = self.observer.on_next(elem)

            if isinstance(inner_ack, ContinueAck):
                ack_subject.on_next(inner_ack)
            elif isinstance(inner_ack, StopAck):
                ack_subject.on_next(inner_ack)
            elif inner_ack is None:
                raise Exception(f'observer {self.observer} returned None instead of an Ack')
            else:
                inner_ack.subscribe(ack_subject)

        self.schedule_func(action)
        return ack_subject

    def on_error(self, exc):
        self.observer.on_error(exc)

    def on_completed(self):
        def action(_, __):
            self.observer.on_completed()

        self.schedule_func(action)
