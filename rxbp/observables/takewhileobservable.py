from typing import Callable, Any

from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo

from rxbp.typing import ElementType

from rxbp.ack.ackimpl import stop_ack, continue_ack

class TakeWhileObservable(Observable):
    """
    Forwards elements downstream as long as a specified condition for the current element
    is true.

    ``` python
    # take first 5 elements
    first_five = rxbp.range(10).pipe(
        rxbp.op.take_while(lambda v: v<5),
    )
    ```

    The above example creates 10 values `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]` and takes the
    first five values `[0, 1, 2, 3, 4]`.
    """

    def __init__(self, source: Observable, predicate: Callable[[Any], bool]):
        super().__init__()

        self.source = source
        self.predicate = predicate

    def observe(self, observer_info: ObserverInfo):
        observer = observer_info.observer
        predicate = self.predicate

        class TakeWhileObserver(Observer):
            def __init__(self):
                self.ack = continue_ack

            def on_next(self, elem: ElementType):
                if self.ack == stop_ack:
                    return stop_ack

                try:
                    for v in elem:
                        if not predicate(v):
                            self.ack = stop_ack
                            break
                        else:
                            self.ack = observer.on_next([v])
                except Exception as e:
                    observer.on_error(e)
                    return self.ack

                if self.ack == stop_ack:
                    observer.on_completed()
                return self.ack

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                return observer.on_completed()

        takewhile_observer_info = observer_info.copy(TakeWhileObserver())
        return self.source.observe(takewhile_observer_info)
