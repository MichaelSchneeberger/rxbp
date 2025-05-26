from unittest import TestCase

from donotation import do

import continuationmonad

import rxbp
from rxbp.testing.tflowable import init_test_flowable
from rxbp.typing import Observer
from rxbp.testing.tobserver import init_test_observer
from rxbp.testing.trun import test_run
from rxbp.flowabletree.operations.share.flowable import init_share_flowable_node


class TestShare(TestCase):
    def test_normal_case(self):
        scheduler = continuationmonad.init_main_virtual_time_scheduler()

        @do()
        def schedule_source(observer: Observer, _):
            yield continuationmonad.sleep(1, scheduler)
            yield observer.on_next(1)
            yield continuationmonad.sleep(1, scheduler)
            return observer.on_next_and_complete(2)

        source = rxbp.create(schedule_source)

        sink1 = init_test_observer(name="sink1", scheduler=scheduler)
        sink2 = init_test_observer(name="sink2", scheduler=scheduler)

        test_run(
            source=init_share_flowable_node(source),
            sinks=(sink1, sink2),
            scheduler=scheduler,
        )

        scheduler.advance_to(0.5)
        self.assertEqual(sink1.received, [])
        self.assertEqual(sink2.received, [])

        scheduler.advance_to(1.5)
        self.assertEqual(sink1.received, [1])
        self.assertFalse(sink1.is_completed)
        self.assertEqual(sink2.received, [1])

        scheduler.advance_to(2.5)
        self.assertEqual(sink1.received, [1, 2])
        self.assertTrue(sink1.is_completed)
        self.assertEqual(sink2.received, [1, 2])
        self.assertTrue(sink2.is_completed)

        scheduler.advance_to(3.5)

    def test_backpressure_case(self):
        scheduler = continuationmonad.init_main_virtual_time_scheduler()

        @do()
        def schedule_source(observer: Observer, _):
            yield continuationmonad.sleep(1, scheduler)
            yield observer.on_next(1)
            yield continuationmonad.sleep(1, scheduler)
            return observer.on_next_and_complete(2)

        source = rxbp.create(schedule_source)

        sink1 = init_test_observer(name="sink1", scheduler=scheduler)
        sink2 = init_test_observer(name="sink2", scheduler=scheduler)

        test_run(
            source=init_share_flowable_node(source),
            sinks=(sink1, sink2),
            scheduler=scheduler,
        )

        scheduler.advance_to(0.5)
        self.assertEqual(sink1.received, [])
        self.assertEqual(sink2.received, [])

        sink2.request_delay = 2

        scheduler.advance_to(1.5)
        self.assertEqual(sink1.received, [1])
        self.assertEqual(sink2.received, [1])

        scheduler.advance_to(2.5)
        self.assertEqual(sink1.received, [1, 2])
        self.assertTrue(sink1.is_completed)
        self.assertEqual(sink2.received, [1])
        self.assertFalse(sink2.is_completed)

        scheduler.advance_to(3.5)
        self.assertEqual(sink2.received, [1, 2])
        self.assertTrue(sink2.is_completed)

    def test_cancel_one_case(self):
        scheduler = continuationmonad.init_main_virtual_time_scheduler()

        @do()
        def schedule_source(observer: Observer, _):
            yield continuationmonad.sleep(1, scheduler)
            yield observer.on_next(1)
            yield continuationmonad.sleep(1, scheduler)
            return observer.on_next_and_complete(2)

        source = rxbp.create(schedule_source)

        sink1 = init_test_observer(name="sink1", scheduler=scheduler)
        sink2 = init_test_observer(name="sink2", scheduler=scheduler)

        test_run(
            source=init_share_flowable_node(source),
            sinks=(sink1, sink2),
            scheduler=scheduler,
        )

        scheduler.advance_to(0.5)
        self.assertEqual(sink1.received, [])
        self.assertEqual(sink2.received, [])

        scheduler.advance_to(1.5)
        self.assertEqual(sink1.received, [1])
        self.assertFalse(sink1.is_completed)
        self.assertEqual(sink2.received, [1])

        sink2.cancel()

        scheduler.advance_to(2.5)
        self.assertEqual(sink1.received, [1, 2])
        self.assertTrue(sink1.is_completed)

        scheduler.advance_to(3.5)

    def test_cancel_all_case(self):
        scheduler = continuationmonad.init_main_virtual_time_scheduler()

        @do()
        def schedule_source(observer: Observer, _):
            yield continuationmonad.sleep(1, scheduler)
            yield observer.on_next(1)
            yield continuationmonad.sleep(1, scheduler)
            yield observer.on_next(2)
            yield continuationmonad.sleep(1, scheduler)
            return observer.on_next_and_complete(3)

        source = init_test_flowable(schedule_source)

        sink1 = init_test_observer(name="sink1", scheduler=scheduler)
        sink2 = init_test_observer(name="sink2", scheduler=scheduler)

        test_run(
            source=init_share_flowable_node(source),
            sinks=(sink1, sink2),
            scheduler=scheduler,
        )

        scheduler.advance_to(0.5)
        self.assertEqual(sink1.received, [])
        self.assertEqual(sink2.received, [])

        scheduler.advance_to(1.5)
        self.assertEqual(sink1.received, [1])
        self.assertEqual(sink2.received, [1])

        sink2.cancel()

        scheduler.advance_to(2.5)
        self.assertEqual(sink1.received, [1, 2])

        sink1.cancel()

        scheduler.advance_to(3.5)

        self.assertIsNotNone(source.cancellation.is_cancelled())
