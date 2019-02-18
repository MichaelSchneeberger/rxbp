import unittest

from rxbackpressure.ack import Continue
from rxbackpressure.observers.connectablesubscriber import ConnectableSubscriber
from rxbackpressure.observer import Observer
from rxbackpressure.testing.testscheduler import TestScheduler


class TestConnectableSubscriber(unittest.TestCase):

    def setUp(self):
        self.scheduler = TestScheduler()

    def test_should_block_onnext_until_connected(self):
        s: TestScheduler = self.scheduler

        received = []
        was_completed = [False]

        class TestObserver(Observer):
            def on_next(self, v):
                received.append(v)
                return Continue()

            def on_error(self, err):
                pass

            def on_completed(self):
                was_completed[0] = True

        down_stream = ConnectableSubscriber(TestObserver(), scheduler=s)

        f = down_stream.on_next(10)
        down_stream.on_completed()
        s.advance_by(1)

        self.assertFalse(was_completed[0], 'f should not be completed')
        for i in range(9):
            down_stream.push_first(i+1)

        s.advance_by(1)
        self.assertFalse(was_completed[0], 'f should not be completed')
        self.assertEqual(len(received), 0)

        down_stream.connect()
        s.advance_by(1)
        self.assertTrue(was_completed[0], 'f should be completed')
        self.assertEqual(len(received), 10)
        self.assertLessEqual(received, [e+1 for e in range(10)])

    def test_should_emit_pushed_items_immediately_after_connect(self):
        s: TestScheduler = self.scheduler

        received = []
        was_completed = [False]

        class TestObserver(Observer):
            def on_next(self, v):
                received.append(v)
                return Continue()

            def on_error(self, err):
                pass

            def on_completed(self):
                was_completed[0] = True

        down_stream = ConnectableSubscriber(TestObserver(), scheduler=s)

        down_stream.push_first(1)
        down_stream.push_first(2)
        down_stream.connect()
        s.advance_by(1)

        self.assertEqual(len(received), 2)
        self.assertLessEqual(received, [e+1 for e in range(2)])
        self.assertFalse(was_completed[0])

    def test_should_not_allow_push_first_after_connect(self):
        s: TestScheduler = self.scheduler

        class TestObserver(Observer):
            def on_next(self, v):
                return Continue()

            def on_error(self, err):
                pass

            def on_completed(self):
                pass

        down_stream = ConnectableSubscriber(TestObserver(), scheduler=s)

        down_stream.connect()
        s.advance_by(1)

        with self.assertRaises(Exception):
            down_stream.push_first(1)

    def test_should_not_allow_push_first_all_after_connect(self):
        s: TestScheduler = self.scheduler

        class TestObserver(Observer):
            def on_next(self, v):
                return Continue()

            def on_error(self, err):
                pass

            def on_completed(self):
                pass

        down_stream = ConnectableSubscriber(TestObserver(), scheduler=s)

        down_stream.connect()
        s.advance_by(1)

        with self.assertRaises(Exception):
            down_stream.push_first_all(1)

    def test_should_schedule_push_complete(self):
        s: TestScheduler = self.scheduler

        received = []
        was_completed = [False]

        class TestObserver(Observer):
            def on_next(self, v):
                received.append(v)
                return Continue()

            def on_error(self, err):
                pass

            def on_completed(self):
                was_completed[0] = True

        down_stream = ConnectableSubscriber(TestObserver(), scheduler=s)

        down_stream.on_next(10)
        down_stream.push_first(1)
        down_stream.push_first(2)
        down_stream.push_completed()
        down_stream.connect()

        s.advance_by(1)

        self.assertEqual(len(received), 2)
        self.assertLessEqual(received, [e+1 for e in range(2)])
        self.assertTrue(was_completed[0])

    def test_should_not_allow_push_complete_after_connect(self):
        s: TestScheduler = self.scheduler

        class TestObserver(Observer):
            def on_next(self, v):
                return Continue()

            def on_error(self, err):
                pass

            def on_completed(self):
                pass

        down_stream = ConnectableSubscriber(TestObserver(), scheduler=s)

        down_stream.connect()
        s.advance_by(1)

        with self.assertRaises(Exception):
            down_stream._on_completed_or_error()

    # def test_should_schedule_push_error(self):
    #     s: TestScheduler = self.scheduler
    #
    #     received = []
    #     was_completed = [False]
    #
    #     class TestObserver(Observer):
    #         def on_next(self, v):
    #             received.append(v)
    #             return Continue()
    #
    #         def on_error(self, err):
    #             pass
    #
    #         def on_completed(self):
    #             was_completed[0] = True
    #
    #     down_stream = ConnectableSubscriber(TestObserver(), scheduler=s)
    #
    #     down_stream.on_next(10)
    #     down_stream.push_first(1)
    #     down_stream.push_first(2)
    #     down_stream.push_error(Exception('dummy exception'))
    #     down_stream.connect()
    #
    #     # s.advance_by(1)
    #     #
    #     # self.assertEqual(len(received), 2)
    #     # self.assertLessEqual(received, [e+1 for e in range(2)])
    #     # self.assertTrue(was_completed[0])