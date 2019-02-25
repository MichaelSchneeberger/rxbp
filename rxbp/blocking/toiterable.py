from rx import config
from rx.concurrency import CurrentThreadScheduler
from rx.internal import Enumerator

from rxbp.ack import Continue
from rxbp.observers.anonymousobserver import AnonymousObserver


def to_iterable(source, scheduler):
    condition = config["concurrency"].Condition()
    notifications = []

    def send_notification(n):
        condition.acquire()
        notifications.append(n)
        condition.notify()  # signal that a new item is available
        condition.release()

    def on_next(v):
        send_notification(('N', v))
        return Continue()

    def on_error(exc):
        send_notification(('E', exc))

    def on_completed():
        send_notification(('C', None))

    observer = AnonymousObserver(on_next=on_next, on_error=on_error, on_completed=on_completed)

    source.subscribe_observer(observer, scheduler, CurrentThreadScheduler())

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

    return Enumerator(gen())