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
    Forwards elements downstream as long as a specified condition for the current element.

    ``` python
    # take first 5 elements
    first_five = rxbp.range(10).pipe(
        rxbp.op.take_while(lambda v: v<5),
    )
    ```

    The above example creates 10 values `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]` and takes the
    first five values `[0, 1, 2, 3, 4, 5]`.
    """

    def __init__(self, source: Observable, func: Callable[[ValueType], bool]):
        pass

    def observe(self, observer_info: ObserverInfo) -> Disposable:
        pass


class TestControlledZipObservable(TestCaseBase):
    """

    """

    def setUp(self):
        self.scheduler = TestScheduler()
        self.source = TestObservable()
        self.exception = Exception('test')

    def test_empty_observable(self):
        sink = TestObserver()
        obs = TakeWhileObservable(source=self.source, func=lambda v: v)
        obs.observe(ObserverInfo(sink))

        self.source.on_completed()
        self.assertTrue(sink.is_completed, True)

    def test_single_non_matching_element_synchronously(self):
        # `immediate_coninue=None` means to return a `Continue` acknowledgment always
        sink = TestObserver(immediate_coninue=None)

        obs = TakeWhileObservable(source=self.source, func=lambda v: v)
        obs.observe(ObserverInfo(sink))

        ack = self.source.on_next_list([0])
        self.assertIsInstance(ack, Continue)
        self.assertListEqual(sink.received, [])

    def test_single_matching_elements_synchronously(self):
        sink = TestObserver(immediate_coninue=None)

        obs = TakeWhileObservable(source=self.source, func=lambda v: v)
        obs.observe(ObserverInfo(sink))

        ack = self.source.on_next_list([1])
        self.assertIsInstance(ack, Continue)
        self.assertListEqual(sink.received, [1])

        ack = self.source.on_next_list([1])
        self.assertIsInstance(ack, Continue)
        self.assertListEqual(sink.received, [1, 1])

        ack = self.source.on_next_list([0])
        self.assertIsInstance(ack, Stop)
        self.assertListEqual(sink.received, [1, 1])

    def test_list_and_complete_synchronously(self):
        sink = TestObserver(immediate_coninue=None)

        obs = TakeWhileObservable(source=self.source, func=lambda v: v)
        obs.observe(ObserverInfo(sink))

        ack = self.source.on_next_list([1, 1, 1])
        self.assertIsInstance(ack, Continue)
        self.assertListEqual(sink.received, [1, 1, 1])

        self.source.on_completed()
        self.assertListEqual(sink.received, [1, 1, 1])
        self.assertTrue(sink.is_completed)

    def test_list_synchronously(self):
        sink = TestObserver(immediate_coninue=None)

        obs = TakeWhileObservable(source=self.source, func=lambda v: v)
        obs.observe(ObserverInfo(sink))

        ack = self.source.on_next_list([1, 1, 1])
        self.assertIsInstance(ack, Continue)
        self.assertListEqual(sink.received, [1, 1, 1])

        ack = self.source.on_next_list([1, 0, 1])
        self.assertIsInstance(ack, Stop)
        self.assertListEqual(sink.received, [1, 1, 1, 1])
        self.assertTrue(sink.is_completed)

    def test_iterable_synchronously(self):
        sink = TestObserver(immediate_coninue=None)

        obs = TakeWhileObservable(source=self.source, func=lambda v: v)
        obs.observe(ObserverInfo(sink))

        ack = self.source.on_next_iter([1, 1, 1])
        self.assertIsInstance(ack, Continue)
        self.assertListEqual(sink.received, [1, 1, 1])

        ack = self.source.on_next_iter([1, 0, 1])
        self.assertIsInstance(ack, Stop)
        self.assertListEqual(sink.received, [1, 1, 1, 1])
        self.assertTrue(sink.is_completed)

    def test_failure_synchronously(self):
        # `immediate_coninue=None` means to return a `Continue` acknowledgment always
        sink = TestObserver(immediate_coninue=None)

        obs = TakeWhileObservable(source=self.source, func=lambda v: v)
        obs.observe(ObserverInfo(sink))

        def gen_iterable():
            for i in range(10):
                if i==3:
                    raise self.exception
                yield 1

        ack = self.source.on_next_iter(gen_iterable())
        self.assertIsInstance(ack, Continue)
        self.assertListEqual(sink.received, [1, 1, 1])
        self.assertEqual(sink.exception, self.exception)

    def test_failure_after_non_matching_element_synchronously(self):
        # `immediate_coninue=None` means to return a `Continue` acknowledgment always
        sink = TestObserver(immediate_coninue=None)

        obs = TakeWhileObservable(source=self.source, func=lambda v: v)
        obs.observe(ObserverInfo(sink))

        def gen_iterable():
            for i in range(10):
                if i == 2:
                    yield 0
                elif i == 3:
                    raise self.exception
                else:
                    yield 1

        ack = self.source.on_next_iter(gen_iterable())
        self.assertIsInstance(ack, Continue)
        self.assertListEqual(sink.received, [1, 1])
        self.assertEqual(sink.exception, self.exception)
