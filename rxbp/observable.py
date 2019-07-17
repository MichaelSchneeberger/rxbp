from abc import abstractmethod, ABC

from rx.core.typing import Disposable
from rxbp.observer import Observer


class Observable(ABC):
    @abstractmethod
    def observe(self, observer: Observer) -> Disposable:
        """ Makes the observable to start emitting elements

        This function ought be called at most once. There is no logic that prevents it from being called more than
        once. It is the responsibility of the Flowable that implements the Observable to make sure that it is called
        at most once.

        :param observer: downstream observer
        :return: Disposable
        """

        ...
