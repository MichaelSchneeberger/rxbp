from typing import Callable

from rxbp.subscriptablebase import SubscriptableBase


class SubscriptableOperator:
    def __init__(self, func: Callable[[SubscriptableBase], SubscriptableBase]):
        self.func = func

    def __call__(self, obs):
        return self.func(obs)
