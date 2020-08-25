from abc import abstractmethod, ABC

from rx.core.typing import Disposable

from rxbp.observerinfo import ObserverInfo


class ObservableMixin(ABC):
    """
    A stream like object that starts emitting zero or more elements once being
    observed.

    By convention, the `observe` method is called no more than once allowing the
    Observable to have mutable states for increased efficiency.
    """

    @abstractmethod
    def observe(self, observer_info: ObserverInfo) -> Disposable:
        """
        Makes the observable start emitting elements

        The first element emitted ought to be scheduled on the subscribe scheduler.

        This function ought be called at most once. There is no logic that prevents 
        it from being called more than once. It is the responsibility of the Flowable 
        to assert this.

        :param observer_info: downstream observer implementing the `on_next`, `on_completed`
        and `on_error` method called by this observable
        :return: Disposable that cancels the process started by calling `observe`
        """

        ...
