from typing import Callable, Any

from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.takewhileobserver import TakeWhileObserver


class TakeWhileObservable(Observable):
    """
    Forwards elements downstream as long as a specified condition for the
    current element is true.

    ``` python
    # take first 5 elements
    first_five = rxbp.range(10).pipe(
        rxbp.op.take_while(lambda v: v<5),
    )
    ```

    The above example creates 10 values `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]`
    and takes the first five values `[0, 1, 2, 3, 4]`.
    """

    def __init__(
            self,
            source: Observable,
            predicate: Callable[[Any], bool],
    ):
        self.source = source
        self.predicate = predicate

    def observe(self, observer_info: ObserverInfo):
        next_observer_info = observer_info.copy(
            observer=TakeWhileObserver(
                observer=observer_info.observer,
                predicate=self.predicate,
            ),
        )
        return self.source.observe(next_observer_info)
