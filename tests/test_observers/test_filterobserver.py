import unittest

from rxbp.observablesubjects.publishosubject import PublishOSubject
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.filterobserver import FilterObserver
from rxbp.selectors.selectionmsg import SelectNext, SelectCompleted
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestFilterObserver(unittest.TestCase):
    def setUp(self):
        self.scheduler = TestScheduler()
        self.source = TestObservable()
        self.selector = PublishOSubject(scheduler=self.scheduler)

    def test_initialize(self):
        sink = TestObserver()
        FilterObserver(
            observer=sink,
            predicate=lambda _: True,
            selector=self.selector
        )

    def test_on_complete_predicate(self):
        sink = TestObserver()
        observer = FilterObserver(
            observer=sink,
            predicate=lambda v: v > 0,
            selector=self.selector
        )
        self.source.observe(ObserverInfo(observer))

        self.source.on_completed()

        self.assertTrue(sink.is_completed)

    def test_single_elem_not_fulfill_predicate(self):
        sink = TestObserver()
        observer = FilterObserver(
            observer=sink,
            predicate=lambda v: v > 0,
            selector=self.selector
        )
        self.source.observe(ObserverInfo(observer))

        self.source.on_next_single(0)

        self.assertEqual([], sink.received)

    def test_single_elem_fulfill_predicate(self):
        sink = TestObserver()
        observer = FilterObserver(
            observer=sink,
            predicate=lambda v: v > 0,
            selector=self.selector
        )
        self.source.observe(ObserverInfo(observer))

        self.source.on_next_single(1)

        self.assertEqual([1], sink.received)

    def test_batch_fulfill_predicate(self):
        sink = TestObserver()
        observer = FilterObserver(
            observer=sink,
            predicate=lambda v: v > 0,
            selector=self.selector
        )
        self.source.observe(ObserverInfo(observer))

        self.source.on_next_list([0, 1, 0, 2])

        self.assertEqual([1, 2], sink.received)

    def test_multiple_elem_fulfill_predicate(self):
        sink = TestObserver()
        observer = FilterObserver(
            observer=sink,
            predicate=lambda v: v > 0,
            selector=self.selector
        )
        self.source.observe(ObserverInfo(observer))

        self.source.on_next_list([1, 0])
        self.source.on_next_list([0, 2])

        self.assertEqual([1, 2], sink.received)

    def test_on_error_predicate(self):
        sink = TestObserver()
        observer = FilterObserver(
            observer=sink,
            predicate=lambda v: v > 0,
            selector=self.selector
        )
        self.source.observe(ObserverInfo(observer))
        exc = Exception()

        self.source.on_error(exc)

        self.assertEqual(exc, sink.exception)

    def test_selector(self):
        sink1 = TestObserver()
        observer = FilterObserver(
            observer=sink1,
            predicate=lambda v: v > 0,
            selector=self.selector
        )
        self.source.observe(ObserverInfo(observer))

        sink2 = TestObserver()
        observer.selector.observe(ObserverInfo(sink2))

        self.source.on_next_list([1, 0])
        self.source.on_next_list([0, 2])

        assert_types = [
            SelectNext,
            SelectCompleted,
            SelectCompleted,
            SelectCompleted,
            SelectNext,
            SelectCompleted,
        ]

        for type, obj in zip(assert_types, sink2.received):
            self.assertIsInstance(obj, type)
