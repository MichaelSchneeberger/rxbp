from typing import Callable

from rxbp.observable import Observable


class ObservableOperator:
    def __init__(self, func: Callable[[Observable], Observable]):
        self.func = func

    def __call__(self, obs):
        return self.func(obs)
