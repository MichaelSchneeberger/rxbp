import types
from abc import abstractmethod

from rx import config
from rx.concurrency import current_thread_scheduler

from rxbackpressure.core.anonymousbackpressureobserver import AnonymousBackpressureObserver
from rxbackpressure.core.autodetachbackpressureobserver import AutoDetachBackpressureObserver
from rxbackpressure.core.observablebase import ObservableBase


class SubFlowObservable(ObservableBase):
    pass