from rx import Observable
from rx.internal import extensionmethod

from rxbackpressure.subjects.controlledsubject import ControlledSubject

# todo: implement ControlledObservable
@extensionmethod(Observable)
def to_backpressure(self, release_buffer=None):
    subject = ControlledSubject(release_buffer=release_buffer)
    disposable = self.subscribe(subject)
    return subject
