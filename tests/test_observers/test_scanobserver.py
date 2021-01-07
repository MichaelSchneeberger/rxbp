import unittest

from rxbp.init.initobserverinfo import init_observer_info
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.pairwiseobserver import PairwiseObserver
from rxbp.observers.scanobserver import ScanObserver
from rxbp.testing.tobservable import TObservable
from rxbp.testing.tobserver import TObserver


class TestScanObserver(unittest.TestCase):
    def setUp(self) -> None:
        self.source = TObservable()

    def test_initialize(self):
        sink = TObserver()
        ScanObserver(
            observer=sink,
            func=lambda acc, v: acc + v,
            initial=0,
        )

    def test_on_error(self):
        sink = TObserver()
        obs = ScanObserver(
            observer=sink,
            func=lambda acc, v: acc + v,
            initial=0,
        )
        self.source.observe(init_observer_info(observer=obs))
        exc = Exception()

        self.source.on_error(exc)

        self.assertEqual(exc, sink.exception)

    def test_exception_during_on_next(self):
        sink = TObserver()
        obs = ScanObserver(
            observer=sink,
            func=lambda acc, v: acc + v,
            initial=0,
        )
        self.source.observe(init_observer_info(observer=obs))
        exc = Exception()

        def gen_iter():
            yield 1
            raise exc

        self.source.on_next_iter(gen_iter())

        self.assertEqual(exc, sink.exception)

    def test_on_completed(self):
        sink = TObserver()
        obs = ScanObserver(
            observer=sink,
            func=lambda acc, v: acc + v,
            initial=0,
        )
        self.source.observe(init_observer_info(observer=obs))

        self.source.on_completed()

        self.assertTrue(sink.is_completed)

    def test_single_value(self):
        sink = TObserver()
        obs = ScanObserver(
            observer=sink,
            func=lambda acc, v: acc + v,
            initial=0,
        )
        self.source.observe(init_observer_info(observer=obs))

        self.source.on_next_single(1)

        self.assertEqual([1], sink.received)

    def test_single_batch(self):
        sink = TObserver()
        obs = ScanObserver(
            observer=sink,
            func=lambda acc, v: acc + v,
            initial=0,
        )
        self.source.observe(init_observer_info(observer=obs))

        self.source.on_next_list([1, 2, 3])

        self.assertEqual([1, 3, 6], sink.received)

    def test_two_values(self):
        sink = TObserver()
        obs = ScanObserver(
            observer=sink,
            func=lambda acc, v: acc + v,
            initial=0,
        )
        self.source.observe(init_observer_info(observer=obs))
        self.source.on_next_single(1)

        self.source.on_next_single(2)

        self.assertEqual([1, 3], sink.received)

    def test_two_batches(self):
        sink = TObserver()
        obs = ScanObserver(
            observer=sink,
            func=lambda acc, v: acc + v,
            initial=0,
        )
        self.source.observe(init_observer_info(observer=obs))
        self.source.on_next_list([1, 2])

        self.source.on_next_list([3, 4])

        self.assertEqual([1, 3, 6, 10], sink.received)
