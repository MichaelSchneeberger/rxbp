from dataclasses import replace

from dataclass_abc import dataclass_abc

from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo


@dataclass_abc
class ObserverInfoImpl(ObserverInfo):
    observer: Observer
    is_volatile: bool

    def copy(self, **kwargs):
        return replace(self, **kwargs)
