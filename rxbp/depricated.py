from typing import Callable

from rxbp.flowable import Flowable
from rxbp.flowableoperator import FlowableOperator


def share(func: Callable[[Flowable], Flowable]):
    """ Share takes a function and exposes a multi-cast flowable via the function's arguments. The multi-cast
    flowable back-pressures, when the first subscriber back-pressures. In case of more than one subscribers,
    the multi-cast flowable buffers the elements and releases an element when the slowest subscriber back-pressures
    the element.

    :return: flowable returned by the extend function
    """

    def inner_func(source: Flowable) -> Flowable:
        return source.share(func=func)
    return FlowableOperator(inner_func)