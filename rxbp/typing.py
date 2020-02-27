from typing import TypeVar, Union, Iterator, List

# value send over a Flowable
ValueType = TypeVar('ValueType')

# Instead of calling on_next method for each value in a sequence, rxbackpressure
# sends these values in a batch. This can be done either by putting them in a
# list or in an iterator. Putting the values in an iterator is sometimes more
# preferable as the data does not need to be copied from one location to the other.
# But sometimes you cannot avoid buffering data in a list.
ElementType = Union[Iterator[ValueType], List[ValueType]]
