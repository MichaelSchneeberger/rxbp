from typing import Optional, List, Callable

from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo


class AnonymousObservable(Observable):
    def __init__(self, observe_func: Callable[[ObserverInfo], None], transformations: Optional[List]):
        super().__init__(transformations=transformations)

        self._observe_func = observe_func

    def observe(self, observer_info: ObserverInfo):
        return self._observe_func(observer_info)

