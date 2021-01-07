import unittest

from rxbp.init.initobserverinfo import init_observer_info
from rxbp.observers.filterobserver import FilterObserver
from rxbp.testing.tobservable import TObservable
from rxbp.testing.tobserver import TObserver
from rxbp.testing.tscheduler import TScheduler


class TestFilterObserver(unittest.TestCase):
    def setUp(self):
        self.scheduler = TScheduler()
        self.source = TObservable()
        self.exc = Exception()

    def test_initialize(self):
        sink = TObserver()
        FilterObserver(
            observer=sink,
            predicate=lambda _: True,
        )

    def test_on_complete(self):
        sink = TObserver()
        observer = FilterObserver(
            observer=sink,
            predicate=lambda v: v > 0,
        )
        self.source.observe(init_observer_info(observer))

        self.source.on_completed()

        self.assertTrue(sink.is_completed)

    def test_on_error(self):
        sink = TObserver()
        observer = FilterObserver(
            observer=sink,
            predicate=lambda v: v > 0,
        )
        self.source.observe(init_observer_info(observer))

        self.source.on_error(self.exc)

        self.assertEqual(self.exc, sink.exception)

    def test_single_elem_not_fulfill_predicate(self):
        sink = TObserver()
        observer = FilterObserver(
            observer=sink,
            predicate=lambda v: v > 0,
        )
        self.source.observe(init_observer_info(observer))

        self.source.on_next_single(0)

        self.assertEqual([], sink.received)

    def test_single_elem_fulfill_predicate(self):
        sink = TObserver()
        observer = FilterObserver(
            observer=sink,
            predicate=lambda v: v > 0,
        )
        self.source.observe(init_observer_info(observer))

        self.source.on_next_single(1)

        self.assertEqual([1], sink.received)

    def test_single_batch(self):
        sink = TObserver()
        observer = FilterObserver(
            observer=sink,
            predicate=lambda v: v > 0,
        )
        self.source.observe(init_observer_info(observer))

        self.source.on_next_list([0, 1, 0, 2])

        self.assertEqual([1, 2], sink.received)

    def test_multiple_batches(self):
        sink = TObserver()
        observer = FilterObserver(
            observer=sink,
            predicate=lambda v: v > 0,
        )
        self.source.observe(init_observer_info(observer))

        self.source.on_next_list([1, 0])
        self.source.on_next_list([0, 2])

        self.assertEqual([1, 2], sink.received)
