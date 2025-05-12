from unittest import TestCase

from donotation import do

import continuationmonad

import rxbp

from rxbp.testing.tflowable import init_test_flowable
from rxbp.typing import Observer
from rxbp.flowabletree.operations.merge.flowable import init_merge_flowable_node
from rxbp.testing.tobserver import init_test_observer
from rxbp.testing.trun import test_run


class TestMerge(TestCase):

    def test_normal_case(self):
        scheduler = continuationmonad.init_virtual_time_scheduler()

        @do()
        def schedule_source1(observer: Observer, _):
            yield continuationmonad.sleep(2, scheduler)
            return observer.on_next_and_complete(3)

        source1 = rxbp.create(schedule_source1)

        @do()
        def schedule_source2(observer: Observer, _):
            yield continuationmonad.sleep(1, scheduler)
            yield observer.on_next(1)
            yield observer.on_next(2)
            yield continuationmonad.sleep(2, scheduler)
            return observer.on_next_and_complete(4)

        source2 = init_test_flowable(schedule_source2)

        sink = init_test_observer()

        test_run(
            source=init_merge_flowable_node(
                children=(source1, source2),
            ),
            sink=sink,
            scheduler=scheduler,
        )

        scheduler.advance_to(0.5)
        self.assertEqual(sink.received, [])

        scheduler.advance_to(1.5)
        self.assertEqual(sink.received, [1, 2])
        self.assertFalse(sink.is_completed)

        scheduler.advance_to(2.5)
        self.assertEqual(sink.received, [1, 2, 3])

        scheduler.advance_to(3.5)
        self.assertEqual(sink.received, [1, 2, 3, 4])
        self.assertTrue(sink.is_completed)
