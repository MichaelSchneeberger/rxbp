from rx import Observable
from rx.internal import extensionmethod

from rxbackpressure.subjects.bufferedsubject import BufferedSubject


@extensionmethod(Observable)
def to_backpressure(self):
    subject = BufferedSubject()
    disposable = self.subscribe(subject)
    return subject
