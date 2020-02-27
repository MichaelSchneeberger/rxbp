from abc import abstractmethod, ABC

from rx.core.typing import Disposable

from rxbp.observerinfo import ObserverInfo


class Observable(ABC):
    """
    An Observable is an rxbackpressure internal object that uses mutable state where it increases efficiency.

    In most cases, an Observable is initiated (possibly multiple times) when subscribing to a Flowable.
    Like Flowables, Observables are chained together from data source to data sink (e.g. downstream). Calling the
    `observe` method on the sink Observable will result in a series of `observe` method calls from sink to source
    (e.g. upstream), which effectively starts the Observable emitting elements.
    """

    @abstractmethod
    def observe(self, observer_info: ObserverInfo) -> Disposable:
        """
        Makes the observable start emitting elements

        This function ought be called at most once. There is no logic that prevents it from being called more than
        once. It is the responsibility of the Flowable that implements the Observable to make sure that it is called
        at most once.

        :param observer_info: structure containing the downstream observer
        :return: Disposable that cancels the process started by calling `observe`
        """

        ...
