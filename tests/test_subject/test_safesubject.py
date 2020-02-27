import unittest

from rx.disposable import CompositeDisposable

from rxbp.ack.continueack import ContinueAck
from rxbp.multicast.subjects.safeflowablesubject import SafeFlowableSubject
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestSafeSubject(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TestScheduler()
        self.composite_disposable = CompositeDisposable()
        self.sink = TestObserver()
        self.exception = Exception()

    def test_on_next_return_continue_ack(self):
        subject = SafeFlowableSubject(scheduler=self.scheduler, composite_diposable=self.composite_disposable)
        subject.subscribe(observer=self.sink, scheduler=self.scheduler)

        ack = subject.on_next(1)

        self.assertIsInstance(ack, ContinueAck)
        self.assertEqual([1], self.sink.received)

    def test_second_on_next_return_continue_ack(self):
        subject = SafeFlowableSubject(scheduler=self.scheduler, composite_diposable=self.composite_disposable)
        subject.subscribe(observer=self.sink, scheduler=self.scheduler)

        subject.on_next(1)
        ack = subject.on_next(2)

        self.assertIsInstance(ack, ContinueAck)
        self.assertEqual([1, 2], self.sink.received)

    def test_on_completed(self):
        subject = SafeFlowableSubject(scheduler=self.scheduler, composite_diposable=self.composite_disposable)
        subject.subscribe(observer=self.sink, scheduler=self.scheduler)

        subject.on_next(1)
        subject.on_next(2)
        subject.on_completed()

        self.assertTrue(self.sink.is_completed)

    def test_on_next_after_on_completed(self):
        subject = SafeFlowableSubject(scheduler=self.scheduler, composite_diposable=self.composite_disposable)
        subject.subscribe(observer=self.sink, scheduler=self.scheduler)

        subject.on_next(1)
        subject.on_next(2)
        subject.on_completed()
        subject.on_next(3)

        self.assertEqual([1, 2], self.sink.received)

    def test_on_error(self):
        subject = SafeFlowableSubject(scheduler=self.scheduler, composite_diposable=self.composite_disposable)
        subject.subscribe(observer=self.sink, scheduler=self.scheduler)

        subject.on_next(1)
        subject.on_next(2)
        subject.on_error(self.exception)

        self.assertEqual(self.exception, self.sink.exception)

    def test_on_next_after_on_error(self):
        subject = SafeFlowableSubject(scheduler=self.scheduler, composite_diposable=self.composite_disposable)
        subject.subscribe(observer=self.sink, scheduler=self.scheduler)

        subject.on_next(1)
        subject.on_next(2)
        subject.on_error(self.exception)
        subject.on_next(3)

        self.assertEqual([1, 2], self.sink.received)

    def test_subscribe_after_first_on_next(self):
        subject = SafeFlowableSubject(scheduler=self.scheduler, composite_diposable=self.composite_disposable)

        subject.on_next(1)

        with self.assertRaises(Exception):
            subject.subscribe(observer=self.sink, scheduler=self.scheduler)

    # def test_disposable(self):
    #     subject = SafeFlowableSubject(scheduler=self.scheduler, composite_diposable=self.composite_disposable)
    #     disposable = subject.subscribe(observer=self.sink, scheduler=self.scheduler)
    #
    #     disposable.dispose()
    #
    #     subject.on_next(1)
    #
    #     self.assertTrue(self.sink.is_completed)
    #     self.assertEqual([], self.sink.received)
