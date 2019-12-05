from functools import reduce

from rxbp.flowablebase import FlowableBase
from rxbp.flowableoperator import FlowableOperator


def pipe(*operators: FlowableOperator):
    def compose(source: FlowableBase) -> FlowableBase:
        return reduce(lambda obs, op: op(obs), operators, source)
    return compose
