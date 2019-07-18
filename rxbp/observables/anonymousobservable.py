from typing import Optional, List, Callable

from rxbp.observable import Observable
from rxbp.observesubscription import ObserveSubscription


class AnonymousObservable(Observable):
    def __init__(self, observe_func: Callable[[ObserveSubscription], None], transformations: Optional[List]):
        super().__init__(transformations=transformations)

        self._observe_func = observe_func

    def observe(self, subscription: ObserveSubscription):
        return self._observe_func(subscription)

