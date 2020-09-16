import functools
from abc import ABC
from typing import Generic

from rxbp.indexed.indexedflowable import IndexedFlowable
from rxbp.mixins.sharedflowablemixin import SharedFlowableMixin
from rxbp.pipeoperation import PipeOperation
from rxbp.typing import ValueType
from rxbp.utils.getstacklines import get_stack_lines


class IndexedSharedFlowable(
    SharedFlowableMixin,
    IndexedFlowable[ValueType],
    Generic[ValueType],
    ABC,
):

    def pipe(self, *operators: PipeOperation['IndexedSharedFlowable']) -> 'IndexedSharedFlowable':
        stack = get_stack_lines()
        return functools.reduce(lambda obs, op: op(obs), operators, self)._share(stack=stack)
