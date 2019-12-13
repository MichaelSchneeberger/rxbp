from abc import ABC

from rxbp.observable import Observable
from rxbp.observer import Observer


class OSubjectBase(Observable, Observer, ABC):
    pass
