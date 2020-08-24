from typing import TypeVar, Union, Tuple, Iterator, List

from rxbp.flowable import Flowable

MultiCastElemType = TypeVar('MultiCastElemType')

MultiCastItem = Union[Iterator[MultiCastElemType], List[MultiCastElemType]]