import functools
from abc import ABC

from rxbp.flowable import Flowable
from rxbp.mixins.sharedflowablemixin import SharedFlowableMixin
from rxbp.pipeoperation import PipeOperation


class SharedFlowable(
    SharedFlowableMixin,
    Flowable,
    ABC,
):
    def pipe(self, *operators: PipeOperation['SharedFlowable']) -> 'SharedFlowable':
        return functools.reduce(lambda obs, op: op(obs), operators, self)
