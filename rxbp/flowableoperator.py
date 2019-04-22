from typing import Callable

from rxbp.flowablebase import FlowableBase


class FlowableOperator:
    def __init__(self, func: Callable[[FlowableBase], FlowableBase]):
        self.func = func

    def __call__(self, obs):
        return self.func(obs)
