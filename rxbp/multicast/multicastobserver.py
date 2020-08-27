from abc import ABC

from rxbp.multicast.mixins.multicastobservermixin import MultiCastObserverMixin


class MultiCastObserver(MultiCastObserverMixin, ABC):
    """
    The following conventions must hold:

    - `on_next` method can be called simultaneously; the order of
      the `on_next` calls does not need to be guaranteed
    - `on_next` method should call the next `on_next` without delay
      (e.g. without involving a scheduler)
    - `on_completed` method is called only after the last `on_next`
      method call returned
    - `on_error` method can be called any time
    """

    pass
