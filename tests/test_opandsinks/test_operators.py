import unittest

import rxbp
from rxbp.flowable import Flowable
from rxbp.selectors.bases.numericalbase import NumericalBase
from rxbp.selectors.bases.objectrefbase import ObjectRefBase
from rxbp.subscriber import Subscriber
from rxbp.testing.testflowable import TestFlowable
from rxbp.testing.testscheduler import TestScheduler


class TestOperators(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TestScheduler()
        self.left_base = ObjectRefBase('left')
        self.right_base = ObjectRefBase('right')
        self.left = TestFlowable(base=self.left_base)
        self.right = TestFlowable(base=self.right_base)

    def test_buffer(self):
        subscription = Flowable(self.left).pipe(
            rxbp.op.buffer(10)
        ).unsafe_subscribe(Subscriber(self.scheduler, self.scheduler))

        self.assertEqual(self.left_base, subscription.info.base)

    def test_concat(self):
        subscription = Flowable(self.left).pipe(
            rxbp.op.concat(
                self.right
            )
        ).unsafe_subscribe(Subscriber(self.scheduler, self.scheduler))

        # self.assertEqual(self.left_base, subscription.info.base)

    def test_controlled_zip(self):
        subscription = Flowable(self.left).pipe(
            rxbp.op.controlled_zip(
                right=self.right,
            )
        ).unsafe_subscribe(Subscriber(self.scheduler, self.scheduler))

    def test_debug(self):
        subscription = Flowable(self.left).pipe(
            rxbp.op.debug('d1'),
        ).unsafe_subscribe(Subscriber(self.scheduler, self.scheduler))

    def test_default_if_empty(self):
        subscription = Flowable(self.left).pipe(
            rxbp.op.default_if_empty(lazy_val=lambda: 5),
        ).unsafe_subscribe(Subscriber(self.scheduler, self.scheduler))

    def test_do_action(self):
        subscription = Flowable(self.left).pipe(
            rxbp.op.do_action(on_next=print),
        ).unsafe_subscribe(Subscriber(self.scheduler, self.scheduler))

    def test_execute_on(self):
        subscription = Flowable(self.left).pipe(
            rxbp.op.execute_on(self.scheduler)
        ).unsafe_subscribe(Subscriber(self.scheduler, self.scheduler))

    def test_filter(self):
        subscription = Flowable(self.left).pipe(
            rxbp.op.filter(lambda v: True)
        ).unsafe_subscribe(Subscriber(self.scheduler, self.scheduler))

    def test_fast_filter(self):
        subscription = Flowable(self.left).pipe(
            rxbp.op.fast_filter(lambda v: True)
        ).unsafe_subscribe(Subscriber(self.scheduler, self.scheduler))

    def test_first(self):
        subscription = Flowable(self.left).pipe(
            rxbp.op.first(raise_exception=lambda f: f())
        ).unsafe_subscribe(Subscriber(self.scheduler, self.scheduler))

    def test_first_or_default(self):
        subscription = Flowable(self.left).pipe(
            rxbp.op.first_or_default(lazy_val=lambda: 5)
        ).unsafe_subscribe(Subscriber(self.scheduler, self.scheduler))

    def test_flat_map(self):
        subscription = Flowable(self.left).pipe(
            rxbp.op.flat_map(lambda _: Flowable(self.right))
        ).unsafe_subscribe(Subscriber(self.scheduler, self.scheduler))

    def test_map_to_iterator(self):
        subscription = Flowable(self.left).pipe(
            rxbp.op.map_to_iterator(lambda _: [1, 2, 3])
        ).unsafe_subscribe(Subscriber(self.scheduler, self.scheduler))

    def test_match(self):
        left = TestFlowable(base=NumericalBase(5))
        right = TestFlowable(base=NumericalBase(5))

        subscription = Flowable(left).pipe(
            rxbp.op.match(Flowable(right))
        ).unsafe_subscribe(Subscriber(self.scheduler, self.scheduler))

    def test_merge(self):
        subscription = Flowable(self.left).pipe(
            rxbp.op.merge(Flowable(self.right))
        ).unsafe_subscribe(Subscriber(self.scheduler, self.scheduler))