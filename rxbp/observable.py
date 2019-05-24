from abc import abstractmethod, ABC
from typing import Callable, Any, List, Optional, Dict

from rxbp.ack import continue_ack
from rxbp.observer import Observer
from rxbp.observers.anonymousobserver import AnonymousObserver
from rxbp.scheduler import Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler


class Observable(ABC):
    @abstractmethod
    def observe(self, observer: Observer):
        """ Makes the observable to start emitting elements

        This function ought be called at most once. There is no logic that prevents it from being called more than
        once. It is the responsibility of the Flowable that implements the Observable to make sure that it is called
        at most once.

        :param observer: downstream observer
        :return: Disposable
        """

        ...
