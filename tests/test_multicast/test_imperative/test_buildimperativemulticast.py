import unittest

from rx.disposable import CompositeDisposable

import rxbp
from rxbp.flowable import Flowable
from rxbp.multicast.imperative.imperativemulticastbuilder import ImperativeMultiCastBuilder
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.testing.testflowable import TestFlowable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestImperativeMultiCastBuilder(unittest.TestCase):
    def setUp(self) -> None:
        self.source = TestFlowable()
        self.scheduler = TestScheduler()
        self.composite_disposable = CompositeDisposable()
        self.sink1 = TestObserver()
        self.sink2 = TestObserver()
        self.sink3 = TestObserver()

    def test_common_case(self):
        s1 = [None]
        s2 = [None]
        s3 = [None]

        def func(builder: ImperativeMultiCastBuilder):
            builder = ImperativeMultiCastBuilder(
                scheduler=self.scheduler,
                composite_disposable=self.composite_disposable,
            )

            s1[0] = builder.create_multicast_subject()
            s2[0] = builder.create_flowable_subject()
            s3[0] = builder.create_flowable_subject()

            s1[0].to_flowable().subscribe(observer=self.sink1, scheduler=self.scheduler)
            s2[0].subscribe(observer=self.sink2, scheduler=self.scheduler)

            blocking_flowable = Flowable(self.source)

            def output_selector(flowable: Flowable):
                return rxbp.multicast.return_flowable(flowable)

            return builder.return_(
                blocking_flowable=blocking_flowable,
                output_selector=output_selector,
            )

        rxbp.multicast.build_imperative_multicast(
            func=func,
        ).to_flowable().subscribe(observer=self.sink3, scheduler=self.scheduler)

        s2[0].on_next(1)
        self.assertEqual([1], self.sink2.received)

        s1[0].on_next(s3[0])
        s3[0].on_next(10)
        self.assertEqual([1], self.sink2.received)

        self.source.on_next_single(20)
        self.assertEqual([20], self.sink3.received)

        self.assertFalse(self.sink1.is_completed)
        self.assertFalse(self.sink2.is_completed)
        self.assertFalse(self.sink3.is_completed)

        self.source.on_completed()

        self.assertTrue(self.sink1.is_completed)
        self.assertTrue(self.sink2.is_completed)
        self.assertTrue(self.sink3.is_completed)
