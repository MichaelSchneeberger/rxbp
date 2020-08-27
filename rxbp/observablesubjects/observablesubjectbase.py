from abc import ABC

from rxbp.observable import Observable
from rxbp.observer import Observer


class ObservableSubjectBase(Observable, Observer, ABC):
    pass
