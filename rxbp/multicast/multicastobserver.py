from abc import ABC

from rxbp.multicast.mixins.multicastobservermixin import MultiCastObserverMixin


class MultiCastObserver(MultiCastObserverMixin, ABC):
    """
    The following conventions must hold:

    - `on_next` method cannot be called simultaneously; the next
      `on_next` method is called only after the last `on_next` method
      call returned.
    - `on_next` method should call the next `on_next` before returning
      the function, e.g. without involving a scheduler
    - `on_completed` method is called only after the last `on_next`
      method call returned
    - `on_error` method can be called any time
    """

    pass
