from typing import TypeVar, Union, Iterator, List

MultiCastElemType = TypeVar('MultiCastElemType')

MultiCastItem = Union[Iterator[MultiCastElemType], List[MultiCastElemType]]