from abc import ABC, abstractmethod


class Single(ABC):
    """
    A Single is like a rx Observer, but emits a single element only. That is why there
    no on_complete method needed.
    """

    @abstractmethod
    def on_next(self, elem):
        ...

    @abstractmethod
    def on_error(self, exc: Exception):
        """ An `Ack` is potentially run on some `Scheduler`. Therefore, if something fails the exception needs to be
        correctly integrated into the data stream. Raising an exception is unsuitable.
        """

        ...
