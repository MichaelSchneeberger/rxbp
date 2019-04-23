from typing import Generator, Callable, TypeVar


ValueType = TypeVar('ValueType')
ElementType = Callable[[], Generator[ValueType, None, None]]
