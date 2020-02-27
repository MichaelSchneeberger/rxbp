from rxbp.ack.acksubject import AckSubject
from rxbp.ack.continueack import ContinueAck
from rxbp.ack.stopack import StopAck, stop_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.typing import ElementType


class ObserveOnObservable(Observable):
    def __init__(self, source: Observable, scheduler: Scheduler):
        self.source = source
        self.scheduler = scheduler

    def observe(self, observer_info: ObserverInfo):
        observer = observer_info.observer

        class ObserveOnObserver(Observer):
            def __init__(self, scheduler: Scheduler):
                self.scheduler = scheduler
                self.trampoline = TrampolineScheduler()

                if scheduler.is_order_guaranteed:
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
                        observer.on_error(exc)
                        return stop_ack

                def action(_, __):
                    inner_ack = observer.on_next(elem)

                    if isinstance(inner_ack, ContinueAck):
                        ack_subject.on_next(inner_ack)
                    elif isinstance(inner_ack, StopAck):
                        ack_subject.on_next(inner_ack)
                    elif inner_ack is None:
                        raise Exception(f'observer {observer} returned None instead of an Ack')
                    else:
                        inner_ack.subscribe(ack_subject)

                self.schedule_func(action)
                return ack_subject

            def on_error(self, exc):
                # def action(_, __):
                observer.on_error(exc)

                # self.schedule_func(action)

            def on_completed(self):
                def action(_, __):
                    observer.on_completed()

                self.schedule_func(action)

        observe_on_observer = ObserveOnObserver(scheduler=self.scheduler)
        observer_on_subscription = ObserverInfo(observe_on_observer, is_volatile=observer_info.is_volatile)
        return self.source.observe(observer_on_subscription)
