from rx import Observable, AnonymousObservable
from rx.internal import extensionmethod

from rxbackpressure.core.anonymousbackpressureobservable import AnonymousBackpressureObservable
from rxbackpressure.subjects.bufferedsubject import BufferedSubject


@extensionmethod(Observable)
def to_backpressure(self, release_buffer=None, scheduler=None):
    parent_scheduler = scheduler

    def subscribe_func(observer, scheduler):
        scheduler_to_use = parent_scheduler or scheduler
        subject = BufferedSubject(scheduler=scheduler_to_use, release_buffer=release_buffer)
        subject.subscribe(observer)
        disposable = self.subscribe(subject)
        return disposable

    return AnonymousBackpressureObservable(subscribe_func=subscribe_func, name='to_backpressure')


