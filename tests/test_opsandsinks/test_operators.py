import unittest

import rxbp
from rxbp.init.initflowable import init_flowable
from rxbp.init.initsubscriber import init_subscriber
from rxbp.indexed.selectors.bases.objectrefbase import ObjectRefBase
from rxbp.testing.testflowable import TestFlowable
from rxbp.testing.tscheduler import TScheduler


class TestOperators(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TScheduler()
        self.left_base = ObjectRefBase('left')
        self.right_base = ObjectRefBase('right')
        self.left = TestFlowable(base=self.left_base)
        self.right = TestFlowable(base=self.right_base)
        self.subscriber = init_subscriber(self.scheduler, self.scheduler)

    def test_buffer(self):
        subscription = init_flowable(self.left).pipe(
            rxbp.op.buffer(10)
        ).unsafe_subscribe(self.subscriber)

        # self.assertEqual(self.left_base, subscription.info.base)

    def test_concat(self):
        subscription = init_flowable(self.left).pipe(
            rxbp.op.concat(
                self.right
            )
        ).unsafe_subscribe(self.subscriber)

        # self.assertEqual(self.left_base, subscription.info.base)

    def test_controlled_zip(self):
        subscription = init_flowable(self.left).pipe(
            rxbp.op.controlled_zip(
                right=self.right,
            )
        ).unsafe_subscribe(self.subscriber)

    def test_debug(self):
        subscription = init_flowable(self.left).pipe(
            rxbp.op.debug('d1'),
        ).unsafe_subscribe(self.subscriber)

    def test_default_if_empty(self):
        subscription = init_flowable(self.left).pipe(
            rxbp.op.default_if_empty(lazy_val=lambda: 5),
        ).unsafe_subscribe(self.subscriber)

    def test_do_action(self):
        subscription = init_flowable(self.left).pipe(
            rxbp.op.do_action(on_next=print),
        ).unsafe_subscribe(self.subscriber)

    # def test_execute_on(self):
    #     subscription = init_flowable(self.left).pipe(
    #         rxbp.op.execute_on(self.scheduler)
    #     ).unsafe_subscribe(self.subscriber)

    def test_filter(self):
        subscription = init_flowable(self.left).pipe(
            rxbp.op.filter(lambda v: True)
        ).unsafe_subscribe(self.subscriber)

    def test_first(self):
        subscription = init_flowable(self.left).pipe(
            rxbp.op.first()
        ).unsafe_subscribe(self.subscriber)

    def test_first_or_default(self):
        subscription = init_flowable(self.left).pipe(
            rxbp.op.first_or_default(lazy_val=lambda: 5)
        ).unsafe_subscribe(self.subscriber)

    def test_flat_map(self):
        subscription = init_flowable(self.left).pipe(
            rxbp.op.flat_map(lambda _: init_flowable(self.right))
        ).unsafe_subscribe(self.subscriber)

    def test_map_to_iterator(self):
        subscription = init_flowable(self.left).pipe(
            rxbp.op.map_to_iterator(lambda _: [1, 2, 3])
        ).unsafe_subscribe(self.subscriber)

    # def test_match(self):
    #     left = TestFlowable(base=NumericalBase(5))
    #     right = TestFlowable(base=NumericalBase(5))
    #
    #     subscription = init_flowable(left).pipe(
    #         rxbp.op.match(init_flowable(right))
    #     ).unsafe_subscribe(self.subscriber)

    def test_merge(self):
        subscription = init_flowable(self.left).pipe(
            rxbp.op.merge(init_flowable(self.right))
        ).unsafe_subscribe(self.subscriber)