from typing import Callable, Any, Optional

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicasts.impl.debugmulticastimpl import DebugMultiCastImpl
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber


def init_debug_multi_cast(
        source: MultiCastMixin,
        name: str,
        on_next: Optional[Callable[[Any], None]] = None,
        on_completed: Optional[Callable[[], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        on_subscribe: Callable[[MultiCastObserverInfo, MultiCastSubscriber], None] = None,
        verbose: bool = None,
):
    if verbose is None:
        verbose = True

    if verbose:
        on_next_func = on_next or (lambda v: print(f'{name}.on_next {v}'))
        on_error_func = on_error or (lambda exc: print(f'{name}.on_error {exc}'))
        on_completed_func = on_completed or (lambda: print(f'{name}.on_completed'))
        on_subscribe_func = on_subscribe or (lambda v1, v2: print(f'{name}.on_observe {v1}, subscriber = {v2}'))

    else:
        def empty_func0():
            return None

        def empty_func1(v):
            return None

        def empty_func2(v1, v2):
            return None

        on_next_func = on_next or empty_func1
        on_error_func = on_error or empty_func1
        on_completed_func = on_completed or empty_func0
        on_subscribe_func = on_subscribe or empty_func2

    return DebugMultiCastImpl(
        source=source,
        name=name,
        on_next=on_next_func,
        on_completed=on_completed_func,
        on_error=on_error_func,
        on_subscribe=on_subscribe_func,
    )
