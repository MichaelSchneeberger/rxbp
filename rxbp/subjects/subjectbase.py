from abc import ABC

from rxbp.flowablebase import FlowableBase
from rxbp.observer import Observer


class SubjectBase(Base, Observer, ABC):
    pass
