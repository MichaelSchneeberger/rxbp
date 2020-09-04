import functools
from abc import ABC
from dataclasses import dataclass
from typing import Generic

from rxbp.mixins.flowableopmixin import FlowableOpMixin
from rxbp.mixins.flowablesubscribemixin import FlowableSubscribeMixin
from rxbp.mixins.sharedflowablemixin import SharedFlowableMixin
from rxbp.pipeoperation import PipeOperation
from rxbp.scheduler import Scheduler
from rxbp.toiterator import to_iterator
from rxbp.typing import ValueType
from rxbp.utils.getstacklines import get_stack_lines


@dataclass
class Flowable(
    FlowableOpMixin,
    FlowableSubscribeMixin,
    Generic[ValueType],
    ABC,
):
    def pipe(self, *operators: PipeOperation['Flowable']) -> 'Flowable':
        flowable = functools.reduce(lambda obs, op: op(obs), operators, self)

        if isinstance(flowable, SharedFlowableMixin):
            return flowable.share()
        else:
            return flowable

    def run(self, scheduler: Scheduler = None):
        return list(to_iterator(source=self, scheduler=scheduler))

    def share(self) -> 'Flowable':
        stack = get_stack_lines()

        return self._share(stack=stack)
