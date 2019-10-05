from typing import TypeVar, Union, Tuple

from rxbp.flowable import Flowable

DeferType = TypeVar('DeferType', bound=Union[Flowable, Tuple])


MultiCastValue = TypeVar('MultiCastValue')