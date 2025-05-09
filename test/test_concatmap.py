from unittest import TestCase

from donotation import do

import continuationmonad

import rxbp
from rxbp.typing import Observer
from rxbp.flowabletree.operations.concatmap.flowable import ConcatMapFlowable
from rxbp.testing.tobserver import init_test_observer
from rxbp.testing.trun import test_run


class TestConcatMap(TestCase):

    def test_normal_case(self):
        scheduler = continuationmonad.init_virtual_time_scheduler()

        @do()
        def schedule_inner_source1(observer: Observer, _):
            yield continuationmonad.sleep(1, scheduler)
            _ = yield observer.on_next(1)
            yield continuationmonad.sleep(1, scheduler)
            return observer.on_next_and_complete(2)

        inner_source1 = rxbp.create(schedule_inner_source1)

        @do()
        def schedule_inner_source2(observer: Observer, _):
            yield continuationmonad.sleep(1, scheduler)
            _ = yield observer.on_next(3)
            yield continuationmonad.sleep(1, scheduler)
            return observer.on_next_and_complete(4)

        inner_source2 = rxbp.create(schedule_inner_source2)

        @do()
        def schedule_source(observer: Observer, _):
            yield observer.on_next(inner_source1)
            return observer.on_next_and_complete(inner_source2)

        source = rxbp.create(schedule_source)

        sink = init_test_observer()

        test_run(
            source=ConcatMapFlowable(
                child=source,
                func=lambda v: v,
            ),
            sink=sink,
            scheduler=scheduler,
        )

        scheduler.advance_to(1.5)
        self.assertEqual(sink.received, [1])

        scheduler.advance_to(2.5)
        self.assertEqual(sink.received, [1, 2])
        self.assertFalse(sink.is_completed)

        scheduler.advance_to(3.5)
        self.assertEqual(sink.received, [1, 2, 3])

        scheduler.advance_to(4.5)
        self.assertEqual(sink.received, [1, 2, 3, 4])
        self.assertTrue(sink.is_completed)

    def test_delayed_outer_flowable(self):
        scheduler = continuationmonad.init_virtual_time_scheduler()

        @do()
        def schedule_inner_source1(observer: Observer, _):
            return observer.on_next_and_complete(1)

        inner_source1 = rxbp.create(schedule_inner_source1)

        @do()
        def schedule_inner_source2(observer: Observer, _):
            return observer.on_next_and_complete(2)
        
        inner_source2 = rxbp.create(schedule_inner_source2)

        @do()
        def schedule_source(observer: Observer, _):
            yield observer.on_next(inner_source1)
            yield continuationmonad.sleep(1, scheduler)
            return observer.on_next_and_complete(inner_source2)

        source = rxbp.create(schedule_source)

        sink = init_test_observer()

        test_run(
            source=ConcatMapFlowable(
                child=source,
                func=lambda v: v,
            ),
            sink=sink,
            scheduler=scheduler,
        )

        scheduler.advance_to(0.5)
        self.assertEqual(sink.received, [1])

        scheduler.advance_to(1.5)
        self.assertEqual(sink.received, [1, 2])
        self.assertTrue(sink.is_completed)

    def test_nonempty_follows_empty(self):
        scheduler = continuationmonad.init_virtual_time_scheduler()

        @do()
        def schedule_inner_source1(observer: Observer, _):
            yield continuationmonad.sleep(1, scheduler)
            return observer.on_completed()

        inner_source1 = rxbp.create(schedule_inner_source1)

        @do()
        def schedule_inner_source2(observer: Observer, _):
            return observer.on_next_and_complete(1)
        
        inner_source2 = rxbp.create(schedule_inner_source2)

        @do()
        def schedule_source(observer: Observer, _):
            yield observer.on_next(inner_source1)
            return observer.on_next_and_complete(inner_source2)

        source = rxbp.create(schedule_source)

        sink = init_test_observer()

        test_run(
            source=ConcatMapFlowable(
                child=source,
                func=lambda v: v,
            ),
            sink=sink,
            scheduler=scheduler,
        )

        scheduler.advance_to(0.5)
        self.assertEqual(sink.received, [])

        scheduler.advance_to(1.5)
        self.assertEqual(sink.received, [1])
        self.assertTrue(sink.is_completed)
