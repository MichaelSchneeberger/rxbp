import math
from typing import Any, Optional

from rxbp.indexed.flowables.fromemptyindexedflowable import FromEmptyIndexedFlowable
from rxbp.indexed.flowables.fromiterableindexedflowable import FromIterableIndexedFlowable
from rxbp.indexed.flowables.singleelementindexedflowable import SingleElementIndexedFlowable
from rxbp.indexed.indexedflowable import IndexedFlowable
from rxbp.indexed.init.initindexedflowable import init_indexed_flowable
from rxbp.indexed.mixins.indexedflowablemixin import IndexedFlowableMixin
from rxbp.indexed.selectors.flowablebase import FlowableBase
from rxbp.indexed.selectors.bases.numericalbase import NumericalBase
from rxbp.indexed.selectors.bases.objectrefbase import ObjectRefBase
from rxbp.utils.getstacklines import get_stack_lines


def _create_base(base: Optional[Any]) -> FlowableBase:
    if base is not None:
        if isinstance(base, str):
            base = ObjectRefBase(base)
        elif isinstance(base, int):
            base = NumericalBase(base)
        elif isinstance(base, FlowableBase):
            base = base
        else:
            raise Exception(f'illegal base "{base}"')

    return base


def empty():
    """
    create a Flowable emitting no elements
    """

    return init_indexed_flowable(FromEmptyIndexedFlowable())


def from_range(arg1: int, arg2: int = None, batch_size: int = None, base: Any = None):
    """
    Create a Flowable that emits elements defined by the range.

    :param arg1: start identifier
    :param arg2: end identifier
    :param batch_size: determines the number of elements that are sent in a batch
    :param base: the base of the Flowable sequence
    """

    if arg2 is None:
        start_idx = 0
        stop_idx = arg1
    else:
        start_idx = arg1
        stop_idx = arg2

    n_elements = stop_idx - start_idx

    base = _create_base(base)

    if base is None:
        base = NumericalBase(n_elements)

    if batch_size is None:
        return init_indexed_flowable(SingleElementIndexedFlowable(
            lazy_elem=lambda: iter(range(start_idx, stop_idx)),
            base=base,
        ))

    else:
        n_batches = max(math.ceil(n_elements / batch_size) - 1, 0)

        class FromRangeIterable():
            def __iter__(self):
                current_start_idx = start_idx
                current_stop_idx = start_idx

                for idx in range(n_batches):
                    current_stop_idx = current_stop_idx + batch_size

                    yield range(current_start_idx, current_stop_idx)

                    current_start_idx = current_stop_idx

                yield range(current_start_idx, stop_idx)

        iterable = FromRangeIterable()

        return init_indexed_flowable(FromIterableIndexedFlowable(
            iterable=iterable,
            base=base,
        ))


def match(*sources: IndexedFlowable) -> IndexedFlowable:
    """
    Merge the elements of zero or more *Flowables* into a single *Flowable*.

    :param sources: zero or more Flowables whose elements are merged
    """

    assert all(isinstance(source, IndexedFlowableMixin) for source in sources), \
        f'"{sources}" must all be of type IndexedFlowableMixin'

    stack = get_stack_lines()

    if len(sources) == 0:
        return empty()
    else:
        return sources[0].match(*sources[1:], stack=stack)


def return_value(val: Any):
    """
    Create a Flowable that emits a single element.

    :param val: the single element emitted by the Flowable
    """

    return init_indexed_flowable(SingleElementIndexedFlowable(
        lazy_elem=lambda: [val],
        base=NumericalBase(1),
    ))
