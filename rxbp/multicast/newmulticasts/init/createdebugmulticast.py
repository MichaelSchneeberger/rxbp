from typing import Callable, Any, Optional

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.newmulticasts.impl.debugmulticastimpl import DebugMultiCastImpl


def create_debug_multi_cast(
        source: MultiCastMixin,
        name: str,
        on_next: Optional[Callable[[Any], None]] = None,
        on_completed: Optional[Callable[[], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
):
    all_none = not any([on_next, on_completed, on_error])

    if all_none:
        on_next_func = on_next or (lambda v: print('{}.on_next {}'.format(name, v)))
        on_error_func = on_error or (lambda exc: print('{}.on_error {}'.format(name, exc)))
        on_completed_func = on_completed or (lambda: print('{}.on_completed'.format(name)))

    else:
        def empty_func0():
            return None

        def empty_func1(v):
            return None

        on_next_func = on_next or empty_func1
        on_error_func = on_error or empty_func1
        on_completed_func = on_completed or empty_func0

    return DebugMultiCastImpl(
        source=source,
        name=name,
        on_next=on_next_func,
        on_completed=on_completed_func,
        on_error=on_error_func,
    )
