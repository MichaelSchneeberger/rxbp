import threading

from rxbp.flowablebase import FlowableBase
from rxbp.scheduler import Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler


def to_iterator(source: FlowableBase, scheduler: Scheduler = None):
    notifications = []

    def send_notification(n):
        notifications.append(n)

    def on_next(v):
        send_notification(('N', v))

    def on_error(exc):
        send_notification(('E', exc))

    def on_completed():
        send_notification(('C', None))

    subscribe_scheduler = TrampolineScheduler()
    scheduler = scheduler or subscribe_scheduler

    source.subscribe(on_next=on_next, on_error=on_error, on_completed=on_completed, scheduler=scheduler,
                     subscribe_scheduler=subscribe_scheduler)

    def gen():
        while True:
            while True:
                if len(notifications):
                    break

                scheduler.sleep(0.1)

            kind, value = notifications.pop(0)

            if kind == "E":
                raise value

            if kind == "C":
                return  # StopIteration

            yield value

    return gen()