from typing import Callable
from functools import reduce
from .observablebase import ObservableBase


def pipe(*operators: Callable[[ObservableBase], ObservableBase]) -> Callable[[ObservableBase], ObservableBase]:

    def compose(source: ObservableBase) -> ObservableBase:
        return reduce(lambda obs, op: op(obs), operators, source)
    return compose
