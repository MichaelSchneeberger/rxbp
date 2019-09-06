from typing import Generator, Callable, TypeVar, Generic

ValueType = TypeVar('ValueType')
ElementType = Callable[[], Generator[ValueType, None, None]]


class Flowable(Generic[ValueType]):
    pass