from typing import TypeVar, Union, Iterator, List


# value send over a Flowable
ValueType = TypeVar('ValueType')

# On the `Observable` level, values are grouped and send as lists or iterators.
# The reason to have lists and iterators together is that rxbp operators need
# both capabilities, buffer elements in a list and transform a sequence on a
# element by element bases. The former operation does not require to create a
# new cache in memory. Therefore, it is more efficient to send the result as
# an iterator.
ElementType = Union[Iterator[ValueType], List[ValueType]]

# MultiCastBase = TypeVar('BaseType')
