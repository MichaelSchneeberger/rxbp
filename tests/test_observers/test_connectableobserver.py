from rxbp.ack.stopack import StopAck
from rxbp.ack.continueack import ContinueAck
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.testing.testcasebase import TestCaseBase
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestConnectableObserver(TestCaseBase):

    def setUp(self):
        self.scheduler = TestScheduler()
        self.exception = Exception('dummy')

    def test_initialize(self):
        sink = TestObserver()
        ConnectableObserver(sink, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)

    def test_connect_empty(self):
        sink = TestObserver()
        observer = ConnectableObserver(sink, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)

        observer.connect()

        self.assertEqual(0, len(sink.received))

    def test_on_next(self):
        sink = TestObserver()
        observer = ConnectableObserver(sink, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)

        ack = observer.on_next([1])

        self.assertEqual(0, len(sink.received))

    def test_on_next_then_connect(self):
        sink = TestObserver()
        observer = ConnectableObserver(sink, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)
        ack = observer.on_next([1])

        observer.connect()

        self.assertEqual([1], sink.received)
        self.assertIsInstance(ack.value, ContinueAck)

    def test_on_error(self):
        sink = TestObserver()
        observer = ConnectableObserver(sink, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)

        observer.on_error(self.exception)

    def test_on_error_then_continue(self):
        sink = TestObserver()
        observer = ConnectableObserver(sink, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)
        observer.on_error(self.exception)

        observer.connect()

        self.assertEqual(self.exception, sink.exception)

    def test_on_next_on_error_then_connect(self):
        sink = TestObserver()
        observer = ConnectableObserver(sink, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)
        ack = observer.on_next([1])
        observer.on_error(self.exception)

        observer.connect()

        self.assertEqual([1], sink.received)
        self.assertEqual(self.exception, sink.exception)
        self.assertIsInstance(ack.value, ContinueAck)

    def test_on_next_on_error_then_connect_on_next(self):
        sink = TestObserver()
        observer = ConnectableObserver(sink, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)
        ack = observer.on_next([1])
        observer.on_error(self.exception)

        observer.connect()

        ack = observer.on_next([2])

        self.assertEqual([1], sink.received)
        self.assertEqual(self.exception, sink.exception)
        self.assertIsInstance(ack, StopAck)
