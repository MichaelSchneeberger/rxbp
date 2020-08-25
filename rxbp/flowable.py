import functools
from abc import ABC
from dataclasses import dataclass
from typing import Generic

from rxbp.mixins.flowableopmixin import FlowableOpMixin
from rxbp.mixins.flowablesubscribemixin import FlowableSubscribeMixin
from rxbp.pipeoperation import PipeOperation
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
    def pipe(self, *operators: PipeOperation['Flowable']) -> 'Flowable':
        return functools.reduce(lambda obs, op: op(obs), operators, self)

    def run(self, scheduler: Scheduler = None):
        return list(to_iterator(source=self, scheduler=scheduler))
