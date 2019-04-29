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
        ...
