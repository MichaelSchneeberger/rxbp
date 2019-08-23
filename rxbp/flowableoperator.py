from typing import Callable

from rxbp.flowable import Flowable


class FlowableOperator:
    def __init__(self, func: Callable[[Flowable], Flowable]):
        self.func = func

    def __call__(self, obs):
        return self.func(obs)
