import threading

from rxbp.flowablebase import FlowableBase
from rxbp.scheduler import Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler


def to_iterator(source: FlowableBase, scheduler: Scheduler = None):
    condition = threading.Condition()
    notifications = []

    def send_notification(n):
        condition.acquire()
        notifications.append(n)
        condition.notify()  # signal that a new item is available
        condition.release()

    def on_next(v):
        send_notification(('N', v))

    def on_error(exc):
        send_notification(('E', exc))

    def on_completed():
        send_notification(('C', None))

    # observer = AnonymousObserver(on_next_func=on_next, on_error_func=on_error, on_completed_func=on_completed)

    source.subscribe(on_next=on_next, on_error=on_error, on_completed=on_completed, scheduler=scheduler,
                     subscribe_scheduler=TrampolineScheduler())

    def gen():
        while True:
            condition.acquire()
            while not len(notifications):
                condition.wait()
            kind, value = notifications.pop(0)

            if kind == "E":
                raise value

            if kind == "C":
                return  # StopIteration

            condition.release()
            yield value

    return gen()
