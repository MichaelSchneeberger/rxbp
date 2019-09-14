from abc import ABC, abstractmethod

from rxbp.observable import Observable
from rxbp.observer import Observer


class OSubjectBase(Observable, Observer, ABC):
    pass
