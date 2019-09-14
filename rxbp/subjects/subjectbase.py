from abc import ABC

from rxbp.flowablebase import FlowableBase
from rxbp.observer import Observer


class SubjectBase(FlowableBase, Observer, ABC):
    pass
