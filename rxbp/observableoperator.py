from typing import Callable

from rxbp.observablebase import ObservableBase


class ObservableOperator:
    def __init__(self, func: Callable[[ObservableBase], ObservableBase]):
        self.func = func

    def __call__(self, obs):
        return self.func(obs)
