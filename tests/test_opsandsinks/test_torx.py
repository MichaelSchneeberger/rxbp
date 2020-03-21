import unittest

from rx.core.abc import Observer

from rxbp.ack.continueack import ContinueAck
from rxbp.flowable import Flowable
from rxbp.testing.testflowable import TestFlowable
from rxbp.testing.testscheduler import TestScheduler


class TestToRx(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TestScheduler()
        self.source = TestFlowable()

        class TestObserver(Observer):
            def __init__(self):
                self.on_next_buffer = []
                self.on_completed_buffer = []
                self.on_error_buffer = []

            def on_next(self, value):
                self.on_next_buffer.append(value)

            def on_error(self, error):
                self.on_error_buffer.append(error)

            def on_completed(self):
                self.on_completed_buffer.append(True)

        self.sink = TestObserver()

    def test_on_next_on_completed(self):
        Flowable(self.source).to_rx().subscribe(self.sink, scheduler=self.scheduler)

        self.assertEqual(self.scheduler, self.source.subscriber.scheduler.underlying)

        ack = self.source.on_next_list([1, 2, 3])

        self.assertEqual([1, 2, 3], self.sink.on_next_buffer)
        self.assertIsInstance(ack, ContinueAck)

        self.source.on_completed()

        self.assertEqual([True], self.sink.on_completed_buffer)

    def test_on_error(self):
        Flowable(self.source).to_rx().subscribe(self.sink, scheduler=self.scheduler)
        exception = Exception('test')

        self.source.on_error(exception)

        self.assertEqual([exception], self.sink.on_error_buffer)

    def test_dispose(self):
        disposable = Flowable(self.source).to_rx().subscribe(self.sink, scheduler=self.scheduler)
        disposable.dispose()

        self.assertTrue(self.source.observable.is_disposed)
