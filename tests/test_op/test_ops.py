import unittest

import rxbp
from rxbp.ack.continueack import continue_ack, ContinueAck
from rxbp.flowable import Flowable
from rxbp.observerinfo import ObserverInfo
from rxbp.selectors.bases.objectrefbase import ObjectRefBase
from rxbp.subscriber import Subscriber
from rxbp.testing.testflowable import TestFlowable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestOps(unittest.TestCase):
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

    def test_controlled_zip(self):
        subscription = Flowable(self.left).pipe(
            rxbp.op.controlled_zip(
                right=self.right,
            )
        ).unsafe_subscribe(Subscriber(self.scheduler, self.scheduler))

    def test_filter(self):
        subscription = Flowable(self.left).pipe(
            rxbp.op.filter(lambda v: True)
        ).unsafe_subscribe(Subscriber(self.scheduler, self.scheduler))

    def test_fast_filter(self):
        subscription = Flowable(self.left).pipe(
            rxbp.op.fast_filter(lambda v: True)
        ).unsafe_subscribe(Subscriber(self.scheduler, self.scheduler))
