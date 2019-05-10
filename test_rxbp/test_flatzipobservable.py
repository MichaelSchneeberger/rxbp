import unittest

from rx.disposable import Disposable

from rxbp.ack import Continue, Ack
from backup.flatzipobservable import FlatZipObservable
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.scheduler import SchedulerBase
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.testing.testscheduler import TestScheduler



class TestFlatZipObservable(unittest.TestCase):

    def setUp(self):
        self.scheduler = TestScheduler()

    def test_left_and_inner_first(self):
        s: TestScheduler = self.scheduler

        class TestObserver(Observer):
            def __init__(self):
                self.received = []
                self.was_completed = False
                self.ack = None

            def on_next(self, v):
                self.received.append(v)
                self.ack = Ack()
                return self.ack

            def on_error(self, err):
                pass

            def on_completed(self):
                self.was_completed = True

        class TestObservable(Observable):
            def __init__(self):
                self.observer = None

            def on_next(self, v):
                return self.observer.on_next(v)

            def unsafe_subscribe(self, observer: Observer, scheduler: SchedulerBase, subscribe_scheduler: SchedulerBase):
                self.observer = observer
                return Disposable()

        s1 = TestObservable()
        s2 = TestObservable()
        s3 = TestObservable()

        o1 = TestObserver()

        obs = FlatZipObservable(s1, s2, lambda v: v)
        obs.subscribe_observer(o1, s, TrampolineScheduler())

        ack1 = s1.on_next(s3)

        self.assertListEqual(o1.received, [])
        self.assertFalse(ack1.has_value)

        ack3 = s3.on_next(10)

        self.assertListEqual(o1.received, [])
        self.assertFalse(ack3.has_value)

        ack2 = s2.on_next(100)

        self.assertListEqual(o1.received, [(10, 100)])

        o1.ack.on_next(Continue())
        o1.ack.on_completed()

        s.advance_by(1)

        self.assertFalse(ack1.has_value)
        self.assertFalse(ack2.has_value)
        self.assertTrue(ack3.has_value)
        self.assertIsInstance(ack3.value, Continue)

    def test_right_first_left_empty(self):
        s: TestScheduler = self.scheduler

        class TestObserver(Observer):
            def __init__(self):
                self.received = []
                self.was_completed = False
                self.ack = None

            def on_next(self, v):
                self.received.append(v)
                self.ack = Ack()
                return self.ack

            def on_error(self, err):
                pass

            def on_completed(self):
                self.was_completed = True

        class TestObservable(Observable):
            def __init__(self):
                self.observer = None

            def on_next(self, v):
                return self.observer.on_next(v)

            def on_completed(self):
                return self.observer.on_completed()

            def unsafe_subscribe(self, observer: Observer, scheduler: SchedulerBase, subscribe_scheduler: SchedulerBase):
                self.observer = observer
                return Disposable()

        s1 = TestObservable()
        s2 = TestObservable()
        s3 = TestObservable()

        o1 = TestObserver()

        obs = FlatZipObservable(s1, s2, lambda v: v)
        obs.subscribe_observer(o1, s, TrampolineScheduler())

        ack2 = s2.on_next(100)

        self.assertListEqual(o1.received, [])
        self.assertFalse(ack2.has_value)

        ack1 = s1.on_next(s3)

        self.assertListEqual(o1.received, [])
        self.assertFalse(ack1.has_value)

        s3.on_completed()

        self.assertListEqual(o1.received, [])

        self.assertTrue(ack1.has_value)
        self.assertTrue(ack2.has_value)

        ack1 = s1.on_next(s3)
        ack2 = s2.on_next(200)
        s3.on_completed()

        self.assertTrue(ack1.has_value)
        self.assertTrue(ack2.has_value)
        self.assertIsInstance(ack1.value, Continue)
        self.assertIsInstance(ack2.value, Continue)

    def test_left_first_inner_empty(self):
        s: TestScheduler = self.scheduler

        class TestObserver(Observer):
            def __init__(self):
                self.received = []
                self.was_completed = False
                self.ack = None

            def on_next(self, v):
                self.received.append(v)
                self.ack = Ack()
                return self.ack

            def on_error(self, err):
                pass

            def on_completed(self):
                self.was_completed = True

        class TestObservable(Observable):
            def __init__(self):
                self.observer = None

            def on_next(self, v):
                return self.observer.on_next(v)

            def on_completed(self):
                return self.observer.on_completed()

            def unsafe_subscribe(self, observer: Observer, scheduler: SchedulerBase, subscribe_scheduler: SchedulerBase):
                self.observer = observer
                return Disposable()

        s1 = TestObservable()
        s2 = TestObservable()
        s3 = TestObservable()
        s4 = TestObservable()

        o1 = TestObserver()

        obs = FlatZipObservable(s1, s2, lambda v: v)
        obs.subscribe_observer(o1, s, TrampolineScheduler())

        ack1 = s1.on_next(s3)

        self.assertListEqual(o1.received, [])
        self.assertFalse(ack1.has_value)

        s3.on_completed()

        self.assertListEqual(o1.received, [])
        self.assertTrue(ack1.has_value)
        self.assertIsInstance(ack1.value, Continue)

        ack2 = s2.on_next(100)

        self.assertListEqual(o1.received, [])
        self.assertTrue(ack2.has_value)
        self.assertIsInstance(ack2.value, Continue)

        ack1 = s1.on_next(s4)

        self.assertListEqual(o1.received, [])
        self.assertFalse(ack1.has_value)

        ack2 = s2.on_next(200)

        self.assertListEqual(o1.received, [])
        self.assertFalse(ack1.has_value)
        self.assertFalse(ack2.has_value)

        s4.on_completed()

        self.assertListEqual(o1.received, [])
        self.assertTrue(ack1.has_value)
        self.assertTrue(ack2.has_value)
