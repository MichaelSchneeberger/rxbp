import unittest

import rx
from rx import operators as rxop

from rxbp.multicast.rxextensions.liftobservable import LiftObservable
from rxbp.multicast.testing.testrxobservable import TestRxObservable
from rxbp.multicast.testing.testrxobserver import TestRxObserver
from rxbp.testing.testscheduler import TestScheduler


class TestLiftObservable(unittest.TestCase):
    def setUp(self):
        self.source = TestRxObservable()
        self.sink = TestRxObserver()
        self.scheduler = TestScheduler()

        def func(obs: rx.typing.Observable, first):
            return obs

        self.disposable = LiftObservable(
            self.source,
            func=func,
            scheduler=self.scheduler,
        ).pipe(
            rxop.flat_map(lambda s: s)
        ).subscribe(self.sink)

    def test_scheduled_on_next(self):
        self.source.on_next(1)
        self.source.on_next(2)

        self.scheduler.advance_by(1)

        self.assertEqual([1, 2], self.sink.received)
        self.assertFalse(self.sink.is_completed)

    def test_non_scheduled_on_next(self):
        self.source.on_next(1)
        self.source.on_next(2)
        self.scheduler.advance_by(1)

        self.source.on_next(3)
        self.source.on_next(4)

        self.assertEqual([1, 2, 3, 4], self.sink.received)
        self.assertFalse(self.sink.is_completed)

    def test_on_completed(self):
        self.source.on_completed()

        self.assertTrue(self.source.is_disposed)
        self.assertTrue(self.sink.is_completed)

    def test_on_completed_after_on_next(self):
        self.source.on_next(1)
        self.source.on_next(2)
        self.source.on_completed()

        self.scheduler.advance_by(1)

        self.assertEqual([1, 2], self.sink.received)
        self.assertTrue(self.sink.is_completed)

    # def test_dispose(self):
    #     self.source.on_next(1)
    #     self.source.on_next(2)
    #
    #     self.disposable.dispose()
    #
    #     self.assertEqual([], self.sink.received)
    #     self.assertTrue(self.source.is_disposed)

    def test_dispose_without_subscriber(self):
        self.disposable.dispose()

        self.assertTrue(self.source.is_disposed)
