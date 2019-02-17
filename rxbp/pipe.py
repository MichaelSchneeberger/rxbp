from typing import Callable
from functools import reduce
from .observable import Observable


def pipe(*operators: Callable[[Observable], Observable]) -> Callable[[Observable], Observable]:

    def compose(source: Observable) -> Observable:
        return reduce(lambda obs, op: op(obs), operators, source)
    return compose
