from rx import Observable
from rx.internal import extensionmethod

from rxbackpressure.subjects.bufferedsubject import BufferedSubject


@extensionmethod(Observable)
def to_backpressure(self, release_buffer=None, scheduler=None):
    subject = BufferedSubject(scheduler=scheduler, release_buffer=release_buffer)
    disposable = self.subscribe(subject)
    return subject
