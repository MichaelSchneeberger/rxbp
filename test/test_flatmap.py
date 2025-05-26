from unittest import TestCase

from donotation import do

import continuationmonad

import rxbp
from rxbp.testing.tflowable import init_test_flowable
from rxbp.typing import Observer
from rxbp.flowabletree.operations.flatmap.flowable import FlatMapFlowableNode
from rxbp.testing.tobserver import init_test_observer
from rxbp.testing.trun import test_run


class TestFlatMap(TestCase):

    def test_empty(self):
        scheduler = continuationmonad.init_main_virtual_time_scheduler()

        @do()
        def schedule_source(observer: Observer, _):
            yield continuationmonad.sleep(1, scheduler)
            return observer.on_completed()

        source = rxbp.create(schedule_source)

        sink = init_test_observer(scheduler=scheduler)

        test_run(
            source=FlatMapFlowableNode(
                child=source,
                func=lambda v: v,
                stack=tuple(),
            ),
            sinks=(sink,),
            scheduler=scheduler,
        )

        scheduler.advance_to(0.5)
        self.assertEqual(sink.received, [])
        self.assertFalse(sink.is_completed)

        scheduler.advance_to(1.5)
        self.assertEqual(sink.received, [])
        self.assertTrue(sink.is_completed)


    def test_outer_complete_first(self):
        scheduler = continuationmonad.init_main_virtual_time_scheduler()

        @do()
        def schedule_inner_source1(observer: Observer, _):
            yield continuationmonad.sleep(2, scheduler)
            _ = yield observer.on_next(2)
            yield continuationmonad.sleep(2, scheduler)
            return observer.on_next_and_complete(3)

        inner_source1 = rxbp.create(schedule_inner_source1)

        @do()
        def schedule_inner_source2(observer: Observer, _):
            yield continuationmonad.sleep(1, scheduler)
            _ = yield observer.on_next(1)
            yield continuationmonad.sleep(2, scheduler)
            return observer.on_completed()

        inner_source2 = rxbp.create(schedule_inner_source2)

        @do()
        def schedule_source(observer: Observer, _):
            yield observer.on_next(inner_source1)
            return observer.on_next_and_complete(inner_source2)

        source = rxbp.create(schedule_source)

        sink = init_test_observer(scheduler=scheduler)

        test_run(
            source=FlatMapFlowableNode(
                child=source,
                func=lambda v: v,
                stack=tuple(),
            ),
            sinks=(sink,),
            scheduler=scheduler,
        )

        scheduler.advance_to(0.5)
        self.assertEqual(sink.received, [])

        scheduler.advance_to(1.5)
        self.assertEqual(sink.received, [1])

        scheduler.advance_to(2.5)
        self.assertEqual(sink.received, [1, 2])
        self.assertFalse(sink.is_completed)

        scheduler.advance_to(3.5)
        self.assertEqual(sink.received, [1, 2])

        scheduler.advance_to(4.5)
        self.assertEqual(sink.received, [1, 2, 3])
        self.assertTrue(sink.is_completed)

    def test_on_outer_error(self):
        scheduler = continuationmonad.init_main_virtual_time_scheduler()
        exception = Exception('TestException')

        @do()
        def schedule_inner_source(observer: Observer, _):
            yield continuationmonad.sleep(1, scheduler)
            yield observer.on_next(1)
            yield continuationmonad.sleep(2, scheduler)
            return observer.on_next_and_complete(2)

        inner_source = init_test_flowable(schedule_inner_source)

        @do()
        def schedule_source(observer: Observer, _):
            yield observer.on_next(inner_source)
            yield continuationmonad.sleep(2, scheduler)
            return observer.on_error(exception)

        source = init_test_flowable(schedule_source)

        sink = init_test_observer(scheduler=scheduler)

        test_run(
            source=FlatMapFlowableNode(
                child=source,
                func=lambda v: v,
                stack=tuple(),
            ),
            sinks=(sink,),
            scheduler=scheduler,
        )

        scheduler.advance_to(0.5)
        self.assertEqual(sink.received, [])

        scheduler.advance_to(1.5)
        self.assertEqual(sink.received, [1])

        scheduler.advance_to(2.5)
        self.assertEqual(sink.received, [1])
        self.assertEqual(sink.exception, exception)
        self.assertIsNotNone(
            inner_source.cancellation.is_cancelled(), 
        )

    def test_on_inner_error(self):
        scheduler = continuationmonad.init_main_virtual_time_scheduler()
        exception = Exception('TestException')

        @do()
        def schedule_inner_source1(observer: Observer, _):
            yield continuationmonad.sleep(2, scheduler)
            return observer.on_error(exception)

        inner_source1 = init_test_flowable(schedule_inner_source1)

        @do()
        def schedule_inner_source2(observer: Observer, _):
            yield continuationmonad.sleep(1, scheduler)
            _ = yield observer.on_next(1)
            yield continuationmonad.sleep(2, scheduler)
            return observer.on_next_and_complete(2)

        inner_source2 = init_test_flowable(schedule_inner_source2)

        @do()
        def schedule_source(observer: Observer, _):
            yield observer.on_next(inner_source1)
            yield observer.on_next(inner_source2)
            yield continuationmonad.sleep(2, scheduler)
            return observer.on_completed()
        

        outer_source = init_test_flowable(schedule_source)

        sink = init_test_observer(scheduler=scheduler)

        test_run(
            source=FlatMapFlowableNode(
                child=outer_source,
                func=lambda v: v,
                stack=tuple(),
            ),
            sinks=(sink,),
            scheduler=scheduler,
        )

        scheduler.advance_to(0.5)
        self.assertEqual(sink.received, [])

        scheduler.advance_to(1.5)
        self.assertEqual(sink.received, [1])

        scheduler.advance_to(2.5)
        self.assertEqual(sink.received, [1])
        self.assertEqual(sink.exception, exception)
        self.assertIsNotNone(
            inner_source2.cancellation.is_cancelled(), 
        )        
        self.assertIsNotNone(
            outer_source.cancellation.is_cancelled(), 
        )

        scheduler.advance_to(3.5)
        self.assertEqual(sink.received, [1])

    def test_on_inner_error_stop_continuation(self):
        scheduler = continuationmonad.init_main_virtual_time_scheduler()
        exception = Exception('TestException')

        @do()
        def schedule_inner_source1(observer: Observer, _):
            return observer.on_error(exception)

        inner_source1 = init_test_flowable(schedule_inner_source1)

        @do()
        def schedule_inner_source2(observer: Observer, _):
            yield continuationmonad.sleep(1, scheduler=scheduler)
            return observer.on_next_and_complete(1)

        inner_source2 = init_test_flowable(schedule_inner_source2)

        @do()
        def schedule_source(observer: Observer, _):
            yield continuationmonad.sleep(1, scheduler)
            yield observer.on_next(inner_source1)
            return observer.on_next_and_complete(inner_source2)
        
        outer_source = init_test_flowable(schedule_source)

        sink = init_test_observer(scheduler=scheduler)

        test_run(
            source=FlatMapFlowableNode(
                child=outer_source,
                func=lambda v: v,
                stack=tuple(),
            ),
            sinks=(sink,),
            scheduler=scheduler,
        )

        scheduler.advance_to(0.5)
        self.assertEqual(sink.received, [])

        scheduler.advance_to(1.5)
        self.assertEqual(sink.received, [])
        self.assertEqual(sink.exception, exception)

        scheduler.advance_to(2.5)
