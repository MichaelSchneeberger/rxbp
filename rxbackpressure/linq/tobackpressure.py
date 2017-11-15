from rx import Observable
from rx.internal import extensionmethod

from rxbackpressure.subjects.controlledsubject import ControlledSubject


@extensionmethod(Observable)
def to_backpressure(self):
    subject = ControlledSubject()
    disposable = self.subscribe(subject)
    return subject
