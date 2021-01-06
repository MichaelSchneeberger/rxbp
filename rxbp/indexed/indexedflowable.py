import functools
from abc import ABC
from typing import Generic

from rxbp.indexed.mixins.indexedflowableopmixin import IndexedFlowableOpMixin
from rxbp.mixins.flowableabsopmixin import FlowableAbsOpMixin
from rxbp.mixins.sharedflowablemixin import SharedFlowableMixin
from rxbp.pipeoperation import PipeOperation
from rxbp.scheduler import Scheduler
from rxbp.toiterator import to_iterator
from rxbp.typing import ValueType
from rxbp.utils.getstacklines import get_stack_lines


class IndexedFlowable(
    IndexedFlowableOpMixin,
    Generic[ValueType],
    ABC,
):
    """
    The IndexedFlowable is a Flowable with further functionality. The subscription returned when subscribing
    to a IndexedFlowable contains an Observable but also a base-selectors pair. In functional programming,
    the base would be the type of the Flowable that determines the nature of the Flowable sequence. Meaning
    the number of elements in the sequence, the order of the elements, the filter that was applied to them, etc.

    The second aspect of the base-selectors pair are selectors or maps that can convert IndexedFlowable sequences 
    with other bases to a new sequence matching the base of this IndexedFlowable.
    """
    
    def share(self) -> 'IndexedFlowable':
        stack = get_stack_lines()

        return self._share(stack=stack)

    def run(self, scheduler: Scheduler = None):
        return list(to_iterator(source=self, scheduler=scheduler))

    def pipe(self, *operators: PipeOperation[FlowableAbsOpMixin]) -> 'IndexedFlowable':
        flowable = functools.reduce(lambda obs, op: op(obs), operators, self)

        if isinstance(flowable, SharedFlowableMixin):
            return flowable.share()
        else:
            return flowable
