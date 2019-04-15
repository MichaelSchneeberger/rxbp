from typing import Optional, List, Callable

from rxbp.observable import Observable
from rxbp.observer import Observer


class AnonymousObservable(Observable):
    def __init__(self, observe_func: Callable[[Observer], None], transformations: Optional[List]):
        super().__init__(transformations=transformations)

        self._observe_func = observe_func

    def observe(self, observer: Observer):
        return self._observe_func(observer)

