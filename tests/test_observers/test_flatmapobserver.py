import threading
import unittest

from rx.disposable import CompositeDisposable

from rxbp.acknowledgement.continueack import ContinueAck
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.observers.flatmapobserver import FlatMapObserver
from rxbp.states.rawstates.rawflatmapstates import RawFlatMapStates
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestFlatMapObserver(unittest.TestCase):
    def setUp(self):
        self.source = TestObservable()
        self.inner_source_1 = TestObservable()
        self.inner_source_2 = TestObservable()

        self.sink = TestObserver()
        self.scheduler = TestScheduler()
        self.composite_disposable = CompositeDisposable()
        self.exc = Exception()
        self.lock = threading.RLock()

    def test_initialize(self):
        observer = FlatMapObserver(
            composite_disposable=self.composite_disposable,
            func=lambda v: v,
            lock=self.lock,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            state=RawFlatMapStates.InitialState(),
            observer_info=init_observer_info(observer=self.sink)
        )

    def test_happy_case1(self):
        observer = FlatMapObserver(
            composite_disposable=self.composite_disposable,
            func=lambda v: v,
            lock=self.lock,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            state=RawFlatMapStates.InitialState(),
            observer_info=init_observer_info(observer=self.sink),
        )
        self.source.observe(init_observer_info(observer))

        ack1 = self.source.on_next_single(self.inner_source_1)
        self.scheduler.advance_by(1)

        self.assertFalse(ack1.has_value)

    def test_happy_case2(self):
        observer = FlatMapObserver(
            composite_disposable=self.composite_disposable,
            func=lambda v: v,
            lock=self.lock,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            state=RawFlatMapStates.InitialState(),
            observer_info=init_observer_info(observer=self.sink),
        )
        self.source.observe(init_observer_info(observer))
        self.source.on_next_single(self.inner_source_1)
        self.scheduler.advance_by(1)

        ack2 = self.inner_source_1.on_next_iter([1, 2])

        self.assertIsInstance(ack2, ContinueAck)
        self.assertListEqual(self.sink.received, [1, 2])

    def test_happy_case3(self):
        observer = FlatMapObserver(
            composite_disposable=self.composite_disposable,
            func=lambda v: v,
            lock=self.lock,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            state=RawFlatMapStates.InitialState(),
            observer_info=init_observer_info(observer=self.sink),
        )
        self.source.observe(init_observer_info(observer))

        ack1 = self.source.on_next_single(self.inner_source_1)
        self.scheduler.advance_by(1)

        self.inner_source_1.on_completed()

        self.assertIsInstance(ack1.value, ContinueAck)

    def test_happy_case4(self):
        observer = FlatMapObserver(
            composite_disposable=self.composite_disposable,
            func=lambda v: v,
            lock=self.lock,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            state=RawFlatMapStates.InitialState(),
            observer_info=init_observer_info(observer=self.sink),
        )
        self.source.observe(init_observer_info(observer))

        self.source.on_next_single(self.inner_source_1)
        self.scheduler.advance_by(1)
        self.inner_source_1.on_completed()
        self.source.on_next_single(self.inner_source_2)
        self.scheduler.advance_by(1)

        ack2 = self.inner_source_1.on_next_iter([1, 2])

        self.assertIsInstance(ack2, ContinueAck)
        self.assertListEqual(self.sink.received, [1, 2])
