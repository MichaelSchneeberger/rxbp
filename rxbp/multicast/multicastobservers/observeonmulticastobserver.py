from dataclasses import dataclass

from rxbp.acknowledgement.acksubject import AckSubject
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.scheduler import Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.typing import ElementType


@dataclass
class ObserveOnMultiCastObserver(MultiCastObserver):
    next_observer: MultiCastObserver
    scheduler: Scheduler
    source_scheduler: Scheduler

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
                self.next_observer.on_error(exc)
                return

        def action(_, __):
            def subscribe_action(_, __):
                self.next_observer.on_next(elem)
            self.source_scheduler.schedule(subscribe_action)

        self.schedule_func(action)
        return ack_subject

    def on_error(self, exc):
        self.next_observer.on_error(exc)

    def on_completed(self):
        def action(_, __):
            def subscribe_action(_, __):
                self.next_observer.on_completed()
            self.source_scheduler.schedule(subscribe_action)

        self.schedule_func(action)