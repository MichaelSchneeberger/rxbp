from abc import ABC

from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observer import Observer


class SubjectBase(FlowableMixin, Observer, ABC):
    pass
