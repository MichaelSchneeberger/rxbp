from abc import ABC

from rxbp.mixins.flowablebasemixin import FlowableBaseMixin
from rxbp.observer import Observer


class SubjectBase(FlowableBaseMixin, Observer, ABC):
    pass
