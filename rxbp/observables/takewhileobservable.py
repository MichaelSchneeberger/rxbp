from typing import Callable, Any

from rx.core.typing import Disposable
from rxbp.ack.ackimpl import Continue, Stop
from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.testing.testcasebase import TestCaseBase
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testobserversubscribeinner import TestObserverSubscribeInner
from rxbp.testing.testscheduler import TestScheduler
from rxbp.typing import ValueType


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

    def __init__(self, source: Observable, func: Callable[[ValueType], bool]):
        source.observer = TestObserver()

    def observe(self, observer_info: ObserverInfo) -> Disposable:
        if observer_info.immediate_continue is None:

        sink = observer_info.observer
        sink.is_completed = True
