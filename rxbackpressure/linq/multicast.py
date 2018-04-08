from rx import Observable
from rx.internal import extensionmethod

from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.core.connectablebackpressureobservable import ConnectableBackpressureObservable, \
    ConnectableSubFlowObservable
from rxbackpressure.core.subflowobservable import SubFlowObservable
from rxbackpressure.subjects.syncedsubject import SyncedSubject


@extensionmethod(BackpressureObservable)
def multicast(self, subject=None):

    subject = subject or SyncedSubject()
    return ConnectableBackpressureObservable(self, subject)


@extensionmethod(SubFlowObservable)
def multicast(self, subject=None):

    subject = subject or SyncedSubject()
    return ConnectableSubFlowObservable(self, subject)
