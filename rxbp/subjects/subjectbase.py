from abc import ABC, abstractmethod

from rxbp.observable import Observable
from rxbp.observer import Observer


class SubjectBase(Observable, Observer, ABC):
    pass
