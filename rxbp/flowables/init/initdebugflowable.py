from traceback import FrameSummary
from typing import Any, Callable, List

from rxbp.acknowledgement.ack import Ack
from rxbp.flowables.debugflowable import DebugFlowable
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observerinfo import ObserverInfo
from rxbp.subscriber import Subscriber


def init_debug_flowable(
        source: FlowableMixin,
        name: str,
        on_next: Callable[[Any], Ack] = None,
        on_completed: Callable[[], None] = None,
        on_error: Callable[[Exception], None] = None,
        on_sync_ack: Callable[[Ack], None] = None,
        on_async_ack: Callable[[Ack], None] = None,
        on_observe: Callable[[ObserverInfo], None] = None,
        on_subscribe: Callable[[Subscriber], None] = None,
        on_raw_ack: Callable[[Ack], None] = None,
        verbose: bool = None,
        stack: List[FrameSummary] = None,
):
    """
    Print debug messages to the console when providing the `name` argument

    :on_next: customize the on next debug console print
    """

    if verbose is None:
        verbose = True

    if verbose:
        on_next_func = on_next or (lambda v: print(f'{name}.on_next {v}'))
        on_error_func = on_error or (lambda exc: print(f'{name}.on_error {exc}'))
        on_completed_func = on_completed or (lambda: print(f'{name}.on_completed'))
        on_observe_func = on_observe or (lambda v: print(f'{name}.on_observe {v}'))
        on_subscribe_func = on_subscribe or (lambda v: print(f'{name}.on_subscribe {v}'))
        on_sync_ack_func = on_sync_ack or (lambda v: print(f'{name}.on_sync_ack {v}'))
        on_async_ack_func = on_async_ack or (lambda v: print(f'{name}.on_async_ack {v}'))
        on_raw_ack_func = on_raw_ack or (lambda v: print(f'{name}.on_raw_ack {v}'))

    else:
        def empty_func0():
            return None

        def empty_func1(v):
            return None

        on_next_func = on_next or empty_func1
        on_error_func = on_error or empty_func1
        on_completed_func = on_completed or empty_func0
        on_observe_func = on_observe or empty_func1
        on_subscribe_func = on_subscribe or empty_func1
        on_sync_ack_func = on_sync_ack or empty_func1
        on_async_ack_func = on_async_ack or empty_func1
        on_raw_ack_func = on_raw_ack or empty_func1

    return DebugFlowable(
        source=source,
        name=name,
        on_next=on_next_func,
        on_error=on_error_func,
        on_completed=on_completed_func,
        on_observe=on_observe_func,
        on_subscribe=on_subscribe_func,
        on_sync_ack=on_sync_ack_func,
        on_async_ack=on_async_ack_func,
        on_raw_ack=on_raw_ack_func,
        stack=stack,
    )
