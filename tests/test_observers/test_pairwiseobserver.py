import unittest

from rxbp.observerinfo import ObserverInfo
from rxbp.observers.pairwiseobserver import PairwiseObserver
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver


class TestPairwiseObserver(unittest.TestCase):
    def setUp(self) -> None:
        self.source = TestObservable()
        self.sink = TestObserver()

    def test_initialize(self):
        PairwiseObserver(self.sink)

    def test_first_single_elem(self):
        obs = PairwiseObserver(self.sink)
        self.source.observe(ObserverInfo(observer=obs))

        self.source.on_next_single(1)

        self.assertEqual(1, obs.last_elem)
        self.assertEqual([], self.sink.received)

    def test_first_multiple_elem(self):
        obs = PairwiseObserver(self.sink)
        self.source.observe(ObserverInfo(observer=obs))

        self.source.on_next_list([1, 2, 3])

        self.assertEqual([(1, 2), (2, 3)], self.sink.received)

    def test_second_single_elem(self):
        obs = PairwiseObserver(self.sink)
        self.source.observe(ObserverInfo(observer=obs))
        self.source.on_next_single(1)

        self.source.on_next_single(2)

        self.assertEqual([(1, 2)], self.sink.received)

    def test_second_multiple_elem(self):
        obs = PairwiseObserver(self.sink)
        self.source.observe(ObserverInfo(observer=obs))
        self.source.on_next_list([1, 2])

        self.source.on_next_list([3, 4])

        self.assertEqual([(1, 2), (2, 3), (3, 4)], self.sink.received)

    def test_on_error(self):
        obs = PairwiseObserver(self.sink)
        self.source.observe(ObserverInfo(observer=obs))
        exc = Exception()

        self.source.on_error(exc)

        self.assertEqual(exc, self.sink.exception)

    def test_exception_during_on_next(self):
        obs = PairwiseObserver(self.sink)
        self.source.observe(ObserverInfo(observer=obs))
        exc = Exception()

        def gen_iter():
            yield 1
            raise exc

        self.source.on_next_iter(gen_iter())

        self.assertEqual(exc, self.sink.exception)

    def test_on_completed(self):
        obs = PairwiseObserver(self.sink)
        self.source.observe(ObserverInfo(observer=obs))

        self.source.on_completed()

        self.assertTrue(self.sink.is_completed)
