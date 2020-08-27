from abc import ABC

from rxbp.mixins.observermixin import ObserverMixin


class Observer(ObserverMixin, ABC):
    """
    The following conventions must hold:

    - `on_next` method cannot be called simultaneously; the next
      `on_next` method is called only after the last `on_next` method
      call returned.
    - `on_completed` method is called only after the last `on_next`
      method call returned
    - `on_error` method can be called any time
    """
