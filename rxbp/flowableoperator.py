from typing import Callable

from rxbp.flowableopmixin import FlowableOpMixin


class FlowableOperator:
    def __init__(self, func: Callable[[FlowableOpMixin], FlowableOpMixin]):
        self.func = func

    def __call__(self, flowable: FlowableOpMixin):
        return self.func(flowable)
