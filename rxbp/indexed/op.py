from typing import Any

from rxbp.indexed.indexedflowable import IndexedFlowable
from rxbp.indexed.selectors.flowablebase import FlowableBase
from rxbp.pipeoperation import PipeOperation
from rxbp.utils.getstacklines import get_stack_lines


def debug_base(
        base: Any,
        name: str = None,
):
    stack = get_stack_lines()

    def op_func(source: IndexedFlowable):
        return source.debug_base(
            base=base,
            stack=stack,
            name=name,
        )

    return PipeOperation(op_func)


def match(
        *others: IndexedFlowable,
):
    """
    Create a new Flowable from this and other Flowables by first filtering and duplicating (if necessary)
    the elements of each Flowable and zip the resulting Flowable sequences together.

    :param sources: other Flowables that get matched with this Flowable.
    """

    stack = get_stack_lines()

    def op_func(left: IndexedFlowable):
        return left.match(
            *others,
            stack=stack,
        )

    return PipeOperation(op_func)


def set_base(val: Any):
    """
    Overwrite the base of the current Flowable sequence.
    """

    def op_func(source: IndexedFlowable):
        return source.set_base(val=val)

    return PipeOperation(op_func)
