from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic

from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.mixins.flowableopmixin import FlowableOpMixin
from rxbp.mixins.flowablesubscribemixin import FlowableSubscribeMixin
from rxbp.scheduler import Scheduler
from rxbp.toiterator import to_iterator
from rxbp.typing import ValueType


@dataclass
class Flowable(
    FlowableOpMixin,
    FlowableSubscribeMixin,
    Generic[ValueType],
    ABC,
):
    @abstractmethod
    def _copy(self, underlying: FlowableMixin, *args, **kwargs) -> 'Flowable':
        ...

    def run(self, scheduler: Scheduler = None):
        return list(to_iterator(source=self, scheduler=scheduler))
